package controllers

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/lmika/dynamo-browse/internal/common/ui/events"
	"github.com/lmika/dynamo-browse/internal/dynamo-browse/models"
	"github.com/lmika/dynamo-browse/internal/dynamo-browse/models/attrcodec"
	"github.com/lmika/dynamo-browse/internal/dynamo-browse/models/attrutils"
	"github.com/lmika/dynamo-browse/internal/dynamo-browse/models/queryexpr"
	"github.com/lmika/dynamo-browse/internal/dynamo-browse/models/serialisable"
	"github.com/lmika/dynamo-browse/internal/dynamo-browse/services"
	"github.com/lmika/dynamo-browse/internal/dynamo-browse/services/inputhistory"
	"github.com/lmika/dynamo-browse/internal/dynamo-browse/services/itemrenderer"
	"github.com/lmika/dynamo-browse/internal/dynamo-browse/services/viewsnapshot"
	bus "github.com/lmika/events"
	"github.com/pkg/errors"
)

type resultSetUpdateOp int

const (
	resultSetUpdateInit resultSetUpdateOp = iota
	resultSetUpdateQuery
	resultSetUpdateFilter
	resultSetUpdateSnapshotRestore
	resultSetUpdateRescan
	resultSetUpdateTouch
	resultSetUpdateNextPage
	resultSetUpdateScript
	resultSetUpdateResort
)

type MarkOp int

const (
	MarkOpMark MarkOp = iota
	MarkOpUnmark
	MarkOpToggle
)

const (
	queryInputHistoryCategory  = "queries"
	filterInputHistoryCategory = "filters"
)

type TableReadController struct {
	tableService        TableReadService
	workspaceService    *viewsnapshot.ViewSnapshotService
	itemRendererService *itemrenderer.Service
	jobController       *JobsController
	inputHistoryService *inputhistory.Service
	eventBus            *bus.Bus
	tableName           string
	loadFromLastView    bool
	pasteboardProvider  services.PasteboardProvider
	relatedItemSupplier RelatedItemSupplier

	// state
	mutex *sync.Mutex
	state *State
}

func NewTableReadController(
	state *State,
	tableService TableReadService,
	workspaceService *viewsnapshot.ViewSnapshotService,
	itemRendererService *itemrenderer.Service,
	jobController *JobsController,
	inputHistoryService *inputhistory.Service,
	eventBus *bus.Bus,
	pasteboardProvider services.PasteboardProvider,
	relatedItemSupplier RelatedItemSupplier,
	tableName string,
) *TableReadController {
	return &TableReadController{
		state:               state,
		tableService:        tableService,
		workspaceService:    workspaceService,
		itemRendererService: itemRendererService,
		jobController:       jobController,
		inputHistoryService: inputHistoryService,
		eventBus:            eventBus,
		tableName:           tableName,
		pasteboardProvider:  pasteboardProvider,
		relatedItemSupplier: relatedItemSupplier,
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
		return c.ListTables(true)
	} else {
		return c.ScanTable(c.tableName)
	}
}

func (c *TableReadController) ListTables(quitIfNoTable bool) tea.Msg {
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
				if tableName == "" {
					if quitIfNoTable {
						return tea.Quit()
					}
					return events.StatusMsg("No table selected")
				}

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
		if resultSet != nil {
			resultSet = c.tableService.Filter(resultSet, c.state.Filter())
		}

		return resultSet, err
	}).OnEither(c.handleResultSetFromJobResult(c.state.Filter(), true, false, resultSetUpdateInit)).Submit()
}

func (c *TableReadController) SortResultSet(newCriteria models.SortCriteria) {
	c.state.withResultSet(func(rs *models.ResultSet) {
		rs.Sort(newCriteria.Append(models.PKSKSortFilter(rs.TableInfo)))
	})
	c.eventBus.Fire(newResultSetEvent, c.state.resultSet, resultSetUpdateResort)
}

func (c *TableReadController) PromptForQuery() tea.Msg {
	return events.PromptForInputMsg{
		Prompt:  "query: ",
		History: c.inputHistoryService.Iter(context.Background(), queryInputHistoryCategory),
		OnDone: func(value string) tea.Msg {
			resultSet := c.state.ResultSet()
			if resultSet == nil {
				return events.StatusMsg("Result-set is nil")
			}

			var q *queryexpr.QueryExpr
			if value != "" {
				var err error
				q, err = queryexpr.Parse(value)
				if err != nil {
					return events.Error(err)
				}
				q = q.WithCurrentResultSet(resultSet).WithPasteboardController(c.pasteboardProvider)
			}

			return c.runQuery(resultSet.TableInfo, q, "", true, nil)
		},
	}
}

func (c *TableReadController) runQuery(
	tableInfo *models.TableInfo,
	query *queryexpr.QueryExpr,
	newFilter string,
	pushSnapshot bool,
	exclusiveStartKey map[string]types.AttributeValue,
) tea.Msg {
	if query == nil {
		return NewJob(c.jobController, "Scanning…", func(ctx context.Context) (*models.ResultSet, error) {
			newResultSet, err := c.tableService.ScanOrQuery(context.Background(), tableInfo, nil, exclusiveStartKey)

			if newResultSet != nil && newFilter != "" {
				newResultSet = c.tableService.Filter(newResultSet, newFilter)
			}

			return newResultSet, err
		}).OnEither(c.handleResultSetFromJobResult(newFilter, pushSnapshot, false, resultSetUpdateQuery)).Submit()
	}

	return c.doIfNoneDirty(func() tea.Msg {
		return NewJob(c.jobController, "Running query…", func(ctx context.Context) (*models.ResultSet, error) {
			newResultSet, err := c.tableService.ScanOrQuery(context.Background(), tableInfo, query, exclusiveStartKey)

			if newFilter != "" && newResultSet != nil {
				newResultSet = c.tableService.Filter(newResultSet, newFilter)
			}
			return newResultSet, err
		}).OnEither(c.handleResultSetFromJobResult(newFilter, pushSnapshot, false, resultSetUpdateQuery)).Submit()
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
		newResultSet, err := c.tableService.ScanOrQuery(ctx, resultSet.TableInfo, query, resultSet.LastEvaluatedKey)
		if newResultSet != nil {
			newResultSet = c.tableService.Filter(newResultSet, c.state.Filter())
		}

		return newResultSet, err
	}).OnEither(c.handleResultSetFromJobResult(c.state.Filter(), pushBackstack, false, op)).Submit()
}

func (c *TableReadController) setResultSetAndFilter(resultSet *models.ResultSet, filter string, pushBackstack bool, op resultSetUpdateOp) tea.Msg {
	if resultSet != nil && pushBackstack {
		details := serialisable.ViewSnapshotDetails{
			TableName: resultSet.TableInfo.Name,
			Filter:    filter,
		}

		if q := resultSet.Query; q != nil {
			if bs, err := q.SerializeToBytes(); err == nil {
				details.Query = bs
				details.QueryHash = q.HashCode()
			} else {
				log.Printf("cannot serialize query to bytes: %v", err)
			}
		}

		if len(resultSet.ExclusiveStartKey) > 0 {
			var err error
			details.ExclusiveStartKey, err = attrcodec.SerializeMapToBytes(resultSet.ExclusiveStartKey)
			if err != nil {
				log.Printf("cannot serialize last evaluated key to byte: %v", err)
			}
		}

		log.Printf("pushing to backstack: table = %v, filter = %v, query_hash = %v",
			details.TableName, details.Filter, details.QueryHash)
		if err := c.workspaceService.PushSnapshot(details); err != nil {
			log.Printf("cannot push snapshot: %v", err)
		}
	}

	c.state.setResultSetAndFilter(resultSet, filter)

	c.eventBus.Fire(newResultSetEvent, resultSet, op)

	return c.state.buildNewResultSetMessage("")
}

func (c *TableReadController) Mark(op MarkOp, where string) tea.Msg {
	var (
		whereExpr *queryexpr.QueryExpr
		err       error
	)

	if where != "" {
		whereExpr, err = queryexpr.Parse(where)
		if err != nil {
			return events.Error(err)
		}
	}

	if err := c.state.withResultSetReturningError(func(resultSet *models.ResultSet) error {
		for i, item := range resultSet.Items() {
			if resultSet.Hidden(i) {
				continue
			}

			if whereExpr != nil {
				res, err := whereExpr.EvalItem(item)
				if err != nil {
					return errors.Wrapf(err, "item %d", i)
				}
				if !attrutils.Truthy(res) {
					continue
				}
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
		return nil
	}); err != nil {
		return events.Error(err)
	}
	return ResultSetUpdated{}
}

func (c *TableReadController) Filter() tea.Msg {
	return events.PromptForInputMsg{
		Prompt:  "filter: ",
		History: c.inputHistoryService.Iter(context.Background(), filterInputHistoryCategory),
		OnDone: func(value string) tea.Msg {
			resultSet := c.state.ResultSet()
			if resultSet == nil {
				return events.StatusMsg("Result-set is nil")
			}

			return NewJob(c.jobController, "Applying Filter…", func(ctx context.Context) (*models.ResultSet, error) {
				newResultSet := c.tableService.Filter(resultSet, value)
				return newResultSet, nil
			}).OnEither(c.handleResultSetFromJobResult(value, true, false, resultSetUpdateFilter)).Submit()
		},
	}
}

func (c *TableReadController) handleResultSetFromJobResult(
	filter string,
	pushbackStack, errIfEmpty bool,
	op resultSetUpdateOp,
) func(newResultSet *models.ResultSet, err error) tea.Msg {
	return func(newResultSet *models.ResultSet, err error) tea.Msg {
		if err == nil {
			if errIfEmpty && newResultSet.NoResults() {
				return events.StatusMsg("No more results")
			}

			return c.setResultSetAndFilter(newResultSet, filter, pushbackStack, op)
		}

		var partialResultsErr models.PartialResultsError
		if errors.As(err, &partialResultsErr) {
			if newResultSet == nil {
				return events.StatusMsg("Operation cancelled")
			}

			return events.Confirm(applyToN("View the ", len(newResultSet.Items()), "item", "items", " returned so far? "), func(yes bool) tea.Msg {
				if yes {
					return c.setResultSetAndFilter(newResultSet, filter, pushbackStack, op)
				}
				return events.StatusMsg("Operation cancelled")
			})
		}

		if newResultSet != nil {
			return c.setResultSetAndFilter(newResultSet, filter, pushbackStack, op)
		}
		return events.Error(err)
	}
}

func (c *TableReadController) ViewBack() tea.Msg {
	viewSnapshot, err := c.workspaceService.ViewBack()
	if err != nil {
		return events.Error(err)
	} else if viewSnapshot == nil {
		return events.StatusMsg("Backstack is empty")
	}

	log.Printf("view back: table = %v, filter = %v, query_hash = %v",
		viewSnapshot.Details.TableName, viewSnapshot.Details.Filter, viewSnapshot.Details.QueryHash)
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

func (c *TableReadController) NextPage() tea.Msg {
	resultSet := c.state.ResultSet()
	if resultSet == nil {
		return events.StatusMsg("Result-set is nil")
	} else if !resultSet.HasNextPage() {
		return events.StatusMsg("No more results")
	}
	currentFilter := c.state.filter

	return NewJob(c.jobController, "Fetching next page…", func(ctx context.Context) (*models.ResultSet, error) {
		return c.tableService.NextPage(ctx, resultSet)
	}).OnEither(c.handleResultSetFromJobResult(currentFilter, true, true, resultSetUpdateNextPage)).Submit()
}

func (c *TableReadController) updateViewToSnapshot(viewSnapshot *serialisable.ViewSnapshot) tea.Msg {
	var err error
	currentResultSet := c.state.ResultSet()

	var query *queryexpr.QueryExpr
	if len(viewSnapshot.Details.Query) > 0 {
		query, err = queryexpr.DeserializeFrom(bytes.NewReader(viewSnapshot.Details.Query))
		if err != nil {
			return err
		}
	}

	var exclusiveStartKey map[string]types.AttributeValue
	if len(viewSnapshot.Details.ExclusiveStartKey) > 0 {
		exclusiveStartKey, err = attrcodec.DeseralizedMapFromBytes(viewSnapshot.Details.ExclusiveStartKey)
		if err != nil {
			return err
		}
	}

	if currentResultSet == nil {
		return NewJob(c.jobController, "Fetching table info…", func(ctx context.Context) (*models.TableInfo, error) {
			tableInfo, err := c.tableService.Describe(context.Background(), viewSnapshot.Details.TableName)
			if err != nil {
				return nil, err
			}
			return tableInfo, nil
		}).OnDone(func(tableInfo *models.TableInfo) tea.Msg {
			return c.runQuery(tableInfo, query, viewSnapshot.Details.Filter, false, exclusiveStartKey)
		}).Submit()
	}

	queryEqualsCurrentQuery := false
	if q, ok := currentResultSet.Query.(*queryexpr.QueryExpr); ok && q != nil {
		queryEqualsCurrentQuery = q.Equal(query)
	}

	if viewSnapshot.Details.TableName == currentResultSet.TableInfo.Name && queryEqualsCurrentQuery {
		return NewJob(c.jobController, "Applying filter…", func(ctx context.Context) (*models.ResultSet, error) {
			return c.tableService.Filter(currentResultSet, viewSnapshot.Details.Filter), nil
		}).OnEither(c.handleResultSetFromJobResult(viewSnapshot.Details.Filter, false, false, resultSetUpdateSnapshotRestore)).Submit()
	}

	return NewJob(c.jobController, "Running query…", func(ctx context.Context) (tea.Msg, error) {
		tableInfo := currentResultSet.TableInfo
		if viewSnapshot.Details.TableName != currentResultSet.TableInfo.Name {
			tableInfo, err = c.tableService.Describe(context.Background(), viewSnapshot.Details.TableName)
			if err != nil {
				return nil, err
			}
		}

		return c.runQuery(tableInfo, query, viewSnapshot.Details.Filter, false, exclusiveStartKey), nil
	}).OnDone(func(m tea.Msg) tea.Msg {
		return m
	}).Submit()
}

func (c *TableReadController) CopyItemToClipboard(idx int) tea.Msg {
	itemCount := 0
	if err := c.state.withResultSetReturningError(func(resultSet *models.ResultSet) error {
		sb := new(strings.Builder)
		_ = applyToMarkedItems(resultSet, idx, func(idx int, item models.Item) error {
			if sb.Len() > 0 {
				fmt.Fprintln(sb, "---")
			}
			c.itemRendererService.RenderItem(sb, resultSet.Items()[idx], resultSet, true)
			itemCount += 1
			return nil
		})

		if err := c.pasteboardProvider.WriteText([]byte(sb.String())); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return events.Error(err)
	}

	return events.StatusMsg(applyToN("", itemCount, "item", "items", " copied to clipboard"))
}
