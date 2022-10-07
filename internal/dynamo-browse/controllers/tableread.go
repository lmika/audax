package controllers

import (
	"context"
	"fmt"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/lmika/audax/internal/common/ui/events"
	"github.com/lmika/audax/internal/dynamo-browse/models"
	"github.com/lmika/audax/internal/dynamo-browse/models/queryexpr"
	"github.com/lmika/audax/internal/dynamo-browse/models/serialisable"
	"github.com/lmika/audax/internal/dynamo-browse/services/itemrenderer"
	"github.com/lmika/audax/internal/dynamo-browse/services/workspaces"
	bus "github.com/lmika/events"
	"github.com/pkg/errors"
	"golang.design/x/clipboard"
	"log"
	"strings"
	"sync"
)

type resultSetUpdateOp int

const (
	resultSetUpdateInit resultSetUpdateOp = iota
	resultSetUpdateQuery
	resultSetUpdateFilter
	resultSetUpdateSnapshotRestore
	resultSetUpdateRescan
	resultSetUpdateTouch
)

type MarkOp int

const (
	MarkOpMark MarkOp = iota
	MarkOpUnmark
	MarkOpToggle
)

type TableReadController struct {
	tableService        TableReadService
	workspaceService    *workspaces.ViewSnapshotService
	itemRendererService *itemrenderer.Service
	jobController       *JobsController
	eventBus            *bus.Bus
	tableName           string
	loadFromLastView    bool

	// state
	mutex         *sync.Mutex
	state         *State
	clipboardInit bool
}

func NewTableReadController(
	state *State,
	tableService TableReadService,
	workspaceService *workspaces.ViewSnapshotService,
	itemRendererService *itemrenderer.Service,
	jobController *JobsController,
	eventBus *bus.Bus,
	tableName string,
) *TableReadController {
	return &TableReadController{
		state:               state,
		tableService:        tableService,
		workspaceService:    workspaceService,
		itemRendererService: itemRendererService,
		jobController:       jobController,
		eventBus:            eventBus,
		tableName:           tableName,
		mutex:               new(sync.Mutex),
	}
}

// Init does an initial scan of the table.  If no table is specified, it prompts for a table, then does a scan.
func (c *TableReadController) Init() tea.Msg {
	// Restore previous view
	if c.loadFromLastView {
		if vs, err := c.workspaceService.ViewRestore(); err == nil && vs != nil {
			return c.updateViewToSnapshot(vs)
		}
	}

	if c.tableName == "" {
		return c.ListTables()
	} else {
		return c.ScanTable(c.tableName)
	}
}

func (c *TableReadController) ListTables() tea.Msg {
	return NewJob(c.jobController, "Listing tables…", func(ctx context.Context) (any, error) {
		tables, err := c.tableService.ListTables(context.Background())
		if err != nil {
			return nil, err
		}
		return tables, nil
	}).OnDone(func(res any) tea.Msg {
		return PromptForTableMsg{
			Tables: res.([]string),
			OnSelected: func(tableName string) tea.Msg {
				return c.ScanTable(tableName)
			},
		}
	}).Submit()
}

func (c *TableReadController) ScanTable(name string) tea.Msg {
	return NewJob(c.jobController, "Scanning…", func(ctx context.Context) (*models.ResultSet, error) {
		tableInfo, err := c.tableService.Describe(ctx, name)
		if err != nil {
			return nil, errors.Wrapf(err, "cannot describe %v", c.tableName)
		}

		resultSet, err := c.tableService.Scan(ctx, tableInfo)
		if err != nil {
			return nil, err
		}
		resultSet = c.tableService.Filter(resultSet, c.state.Filter())

		return resultSet, nil
	}).OnDone(func(rs *models.ResultSet) tea.Msg {
		return c.setResultSetAndFilter(rs, c.state.Filter(), true, resultSetUpdateInit)
	}).Submit()
}

func (c *TableReadController) PromptForQuery() tea.Msg {
	return events.PromptForInputMsg{
		Prompt: "query: ",
		OnDone: func(value string) tea.Msg {
			return NewJob(c.jobController, "Running query…", func(ctx context.Context) (tea.Msg, error) {
				return c.runQuery(c.state.ResultSet().TableInfo, value, "", true), nil
			}).OnDone(func(m tea.Msg) tea.Msg {
				return m
			}).Submit()
		},
	}
}

func (c *TableReadController) runQuery(tableInfo *models.TableInfo, query, newFilter string, pushSnapshot bool) tea.Msg {
	if query == "" {
		newResultSet, err := c.tableService.ScanOrQuery(context.Background(), tableInfo, nil)
		if err != nil {
			return events.Error(err)
		}

		if newFilter != "" {
			newResultSet = c.tableService.Filter(newResultSet, newFilter)
		}

		return c.setResultSetAndFilter(newResultSet, newFilter, pushSnapshot, resultSetUpdateQuery)
	}

	expr, err := queryexpr.Parse(query)
	if err != nil {
		return events.Error(err)
	}

	return c.doIfNoneDirty(func() tea.Msg {
		return NewJob(c.jobController, "Running query…", func(ctx context.Context) (*models.ResultSet, error) {
			newResultSet, err := c.tableService.ScanOrQuery(context.Background(), tableInfo, expr)
			if err != nil {
				return nil, err
			}

			if newFilter != "" {
				newResultSet = c.tableService.Filter(newResultSet, newFilter)
			}
			return newResultSet, nil
		}).OnDone(func(newResultSet *models.ResultSet) tea.Msg {
			return c.setResultSetAndFilter(newResultSet, newFilter, pushSnapshot, resultSetUpdateQuery)
		}).Submit()
	})
}

func (c *TableReadController) doIfNoneDirty(cmd tea.Cmd) tea.Msg {
	var anyDirty = false
	for i := 0; i < len(c.state.ResultSet().Items()); i++ {
		anyDirty = anyDirty || c.state.ResultSet().IsDirty(i)
	}

	if !anyDirty {
		return cmd()
	}

	return events.PromptForInputMsg{
		Prompt: "reset modified items? ",
		OnDone: func(value string) tea.Msg {
			if value != "y" {
				return events.StatusMsg("operation aborted")
			}

			return cmd()
		},
	}
}

func (c *TableReadController) Rescan() tea.Msg {
	return c.doIfNoneDirty(func() tea.Msg {
		resultSet := c.state.ResultSet()
		return c.doScan(resultSet, resultSet.Query, true, resultSetUpdateRescan)
	})
}

func (c *TableReadController) doScan(resultSet *models.ResultSet, query models.Queryable, pushBackstack bool, op resultSetUpdateOp) tea.Msg {
	return NewJob(c.jobController, "Rescan…", func(ctx context.Context) (*models.ResultSet, error) {
		newResultSet, err := c.tableService.ScanOrQuery(ctx, resultSet.TableInfo, query)
		if err != nil {
			return nil, err
		}

		newResultSet = c.tableService.Filter(newResultSet, c.state.Filter())

		return newResultSet, nil
	}).OnDone(func(newResultSet *models.ResultSet) tea.Msg {
		return c.setResultSetAndFilter(newResultSet, c.state.Filter(), pushBackstack, op)
	}).Submit()
}

func (c *TableReadController) setResultSetAndFilter(resultSet *models.ResultSet, filter string, pushBackstack bool, op resultSetUpdateOp) tea.Msg {
	if pushBackstack {
		if err := c.workspaceService.PushSnapshot(resultSet, filter); err != nil {
			log.Printf("cannot push snapshot: %v", err)
		}
	}

	c.state.setResultSetAndFilter(resultSet, filter)

	c.eventBus.Fire(newResultSetEvent, resultSet, op)

	return c.state.buildNewResultSetMessage("")
}

func (c *TableReadController) Mark(op MarkOp) tea.Msg {
	c.state.withResultSet(func(resultSet *models.ResultSet) {
		for i := range resultSet.Items() {
			if resultSet.Hidden(i) {
				continue
			}

			switch op {
			case MarkOpMark:
				resultSet.SetMark(i, true)
			case MarkOpUnmark:
				resultSet.SetMark(i, false)
			case MarkOpToggle:
				resultSet.SetMark(i, !resultSet.Marked(i))
			}
		}
	})
	return ResultSetUpdated{}
}

func (c *TableReadController) Unmark() tea.Msg {
	c.state.withResultSet(func(resultSet *models.ResultSet) {
		for i := range resultSet.Items() {
			resultSet.SetMark(i, false)
		}
	})
	return ResultSetUpdated{}
}

func (c *TableReadController) Filter() tea.Msg {
	return events.PromptForInputMsg{
		Prompt: "filter: ",
		OnDone: func(value string) tea.Msg {
			return NewJob(c.jobController, "Rescan…", func(ctx context.Context) (*models.ResultSet, error) {
				resultSet := c.state.ResultSet()
				newResultSet := c.tableService.Filter(resultSet, value)
				return newResultSet, nil
			}).OnDone(func(newResultSet *models.ResultSet) tea.Msg {
				return c.setResultSetAndFilter(newResultSet, value, true, resultSetUpdateFilter)
			}).Submit()
		},
	}
}

func (c *TableReadController) ViewBack() tea.Msg {
	viewSnapshot, err := c.workspaceService.ViewBack()
	if err != nil {
		return events.Error(err)
	} else if viewSnapshot == nil {
		return events.StatusMsg("Backstack is empty")
	}

	return c.updateViewToSnapshot(viewSnapshot)
}

func (c *TableReadController) ViewForward() tea.Msg {
	viewSnapshot, err := c.workspaceService.ViewForward()
	if err != nil {
		return events.Error(err)
	} else if viewSnapshot == nil {
		return events.StatusMsg("At top of view stack")
	}

	return c.updateViewToSnapshot(viewSnapshot)
}

func (c *TableReadController) updateViewToSnapshot(viewSnapshot *serialisable.ViewSnapshot) tea.Msg {
	var err error
	currentResultSet := c.state.ResultSet()

	if currentResultSet == nil {
		return NewJob(c.jobController, "Running query…", func(ctx context.Context) (tea.Msg, error) {
			tableInfo, err := c.tableService.Describe(context.Background(), viewSnapshot.TableName)
			if err != nil {
				return nil, err
			}
			return c.runQuery(tableInfo, viewSnapshot.Query, viewSnapshot.Filter, false), nil
		}).OnDone(func(m tea.Msg) tea.Msg {
			return m
		}).Submit()
	}

	var currentQueryExpr string
	if currentResultSet.Query != nil {
		currentQueryExpr = currentResultSet.Query.String()
	}

	if viewSnapshot.TableName == currentResultSet.TableInfo.Name && viewSnapshot.Query == currentQueryExpr {
		return NewJob(c.jobController, "Applying filter…", func(ctx context.Context) (*models.ResultSet, error) {
			return c.tableService.Filter(currentResultSet, viewSnapshot.Filter), nil
		}).OnDone(func(newResultSet *models.ResultSet) tea.Msg {
			return c.setResultSetAndFilter(newResultSet, viewSnapshot.Filter, false, resultSetUpdateSnapshotRestore)
		}).Submit()
	}

	return NewJob(c.jobController, "Running query…", func(ctx context.Context) (tea.Msg, error) {
		tableInfo := currentResultSet.TableInfo
		if viewSnapshot.TableName != currentResultSet.TableInfo.Name {
			tableInfo, err = c.tableService.Describe(context.Background(), viewSnapshot.TableName)
			if err != nil {
				return nil, err
			}
		}

		return c.runQuery(tableInfo, viewSnapshot.Query, viewSnapshot.Filter, false), nil
	}).OnDone(func(m tea.Msg) tea.Msg {
		return m
	}).Submit()
}

func (c *TableReadController) CopyItemToClipboard(idx int) tea.Msg {
	if err := c.initClipboard(); err != nil {
		return events.Error(err)
	}

	itemCount := 0
	c.state.withResultSet(func(resultSet *models.ResultSet) {
		sb := new(strings.Builder)
		_ = applyToMarkedItems(resultSet, idx, func(idx int, item models.Item) error {
			if sb.Len() > 0 {
				fmt.Fprintln(sb, "---")
			}
			c.itemRendererService.RenderItem(sb, resultSet.Items()[idx], resultSet, true)
			itemCount += 1
			return nil
		})
		clipboard.Write(clipboard.FmtText, []byte(sb.String()))
	})

	return events.StatusMsg(applyToN("", itemCount, "item", "items", " copied to clipboard"))
}

func (c *TableReadController) initClipboard() error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.clipboardInit {
		return nil
	}

	if err := clipboard.Init(); err != nil {
		return errors.Wrap(err, "unable to enable clipboard")
	}
	c.clipboardInit = true
	return nil
}
