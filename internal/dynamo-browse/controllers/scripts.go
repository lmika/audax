package controllers

import (
	"context"
	"fmt"
	"log"
	"strings"
	"ucl.lmika.dev/ucl"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/lmika/dynamo-browse/internal/common/ui/commandctrl"
	"github.com/lmika/dynamo-browse/internal/common/ui/events"
	"github.com/lmika/dynamo-browse/internal/dynamo-browse/models"
	"github.com/lmika/dynamo-browse/internal/dynamo-browse/models/queryexpr"
	"github.com/lmika/dynamo-browse/internal/dynamo-browse/models/relitems"
	"github.com/lmika/dynamo-browse/internal/dynamo-browse/services/scriptmanager"
	bus "github.com/lmika/events"
	"github.com/pkg/errors"
)

type ScriptController struct {
	scriptManager       *scriptmanager.Service
	tableReadController *TableReadController
	jobController       *JobsController
	settingsController  *SettingsController
	eventBus            *bus.Bus
	sendMsg             func(msg tea.Msg)
}

func NewScriptController(
	scriptManager *scriptmanager.Service,
	tableReadController *TableReadController,
	jobController *JobsController,
	settingsController *SettingsController,
	eventBus *bus.Bus,
) *ScriptController {
	sc := &ScriptController{
		scriptManager:       scriptManager,
		tableReadController: tableReadController,
		jobController:       jobController,
		settingsController:  settingsController,
		eventBus:            eventBus,
	}

	sessionImpl := &sessionImpl{sc: sc, lastSelectedItemIndex: -1}
	scriptManager.SetIFaces(scriptmanager.Ifaces{
		UI:      &uiImpl{sc: sc},
		Session: sessionImpl,
	})

	sessionImpl.subscribeToEvents(eventBus)

	// Setup event handling when settings have changed
	eventBus.On(BusEventSettingsUpdated, func(name, value string) {
		if !strings.HasPrefix(name, "script.") {
			return
		}
		sc.Init()
	})

	return sc
}

func (sc *ScriptController) Init() {
	if lookupPaths, err := sc.settingsController.settings.ScriptLookupFS(); err == nil {
		sc.scriptManager.SetLookupPaths(lookupPaths)
	} else {
		log.Printf("warn: script lookup paths are invalid: %v", err)
	}
}

func (sc *ScriptController) SetMessageSender(sendMsg func(msg tea.Msg)) {
	sc.sendMsg = sendMsg
}

func (sc *ScriptController) LoadScript(filename string) tea.Msg {
	ctx := context.Background()
	plugin, err := sc.scriptManager.LoadScript(ctx, filename)
	if err != nil {
		return events.Error(err)
	}

	return events.StatusMsg(fmt.Sprintf("Script '%v' loaded", plugin.Name()))
}

func (sc *ScriptController) RunScript(filename string) tea.Msg {
	ctx := context.Background()
	if err := sc.scriptManager.StartAdHocScript(ctx, filename, sc.waitAndPrintScriptError()); err != nil {
		return events.Error(err)
	}
	return nil
}

func (sc *ScriptController) waitAndPrintScriptError() chan error {
	errChan := make(chan error)
	go func() {
		if err := <-errChan; err != nil {
			sc.sendMsg(events.Error(err))
		}
	}()
	return errChan
}

func (sc *ScriptController) LookupCommand(name string) commandctrl.Command {
	cmd := sc.scriptManager.LookupCommand(name)
	if cmd == nil {
		return nil
	}

	return func(execCtx commandctrl.ExecContext, args ucl.CallArgs) tea.Msg {
		errChan := sc.waitAndPrintScriptError()
		ctx := context.Background()

		invokeArgs := make([]string, 0)
		for args.NArgs() > 0 {
			var s string
			if err := args.Bind(&s); err == nil {
				invokeArgs = append(invokeArgs, s)
			}
		}

		if err := cmd.Invoke(ctx, invokeArgs, errChan); err != nil {
			return events.Error(err)
		}
		return nil
	}
}

type uiImpl struct {
	sc *ScriptController
}

func (u uiImpl) PrintMessage(ctx context.Context, msg string) {
	u.sc.sendMsg(events.StatusMsg(msg))
}

func (u uiImpl) Prompt(ctx context.Context, msg string) chan string {
	resultChan := make(chan string)
	u.sc.sendMsg(events.PromptForInputMsg{
		Prompt: msg,
		OnDone: func(value string) tea.Msg {
			resultChan <- value
			return nil
		},
		OnCancel: func() tea.Msg {
			close(resultChan)
			return nil
		},
	})
	return resultChan
}

type sessionImpl struct {
	sc                    *ScriptController
	lastSelectedItemIndex int
}

func (s *sessionImpl) subscribeToEvents(bus *bus.Bus) {
	bus.On("ui.new-item-selected", func(rs *models.ResultSet, itemIndex int) {
		s.lastSelectedItemIndex = itemIndex
	})
}

func (s *sessionImpl) SelectedItemIndex(ctx context.Context) int {
	return s.lastSelectedItemIndex
}

func (s *sessionImpl) ResultSet(ctx context.Context) *models.ResultSet {
	return s.sc.tableReadController.state.ResultSet()
}

func (s *sessionImpl) SetResultSet(ctx context.Context, newResultSet *models.ResultSet) {
	state := s.sc.tableReadController.state
	msg := s.sc.tableReadController.setResultSetAndFilter(newResultSet, state.filter, true, resultSetUpdateScript)
	s.sc.sendMsg(msg)
}

func (s *sessionImpl) Query(ctx context.Context, query string, opts scriptmanager.QueryOptions) (*models.ResultSet, error) {
	// Parse the query
	expr, err := queryexpr.Parse(query)
	if err != nil {
		return nil, err
	}

	if opts.NamePlaceholders != nil {
		expr = expr.WithNameParams(opts.NamePlaceholders)
	}
	if opts.ValuePlaceholders != nil {
		expr = expr.WithValueParams(opts.ValuePlaceholders)
	}
	if opts.IndexName != "" {
		expr = expr.WithIndex(opts.IndexName)
	}

	return s.sc.doQuery(ctx, expr, opts)
}

func (s *ScriptController) doQuery(ctx context.Context, expr *queryexpr.QueryExpr, opts scriptmanager.QueryOptions) (*models.ResultSet, error) {
	// Get the table info
	var (
		tableInfo *models.TableInfo
		err       error
	)

	tableName := opts.TableName
	currentResultSet := s.tableReadController.state.ResultSet()

	if tableName != "" {
		// Table specified.  If it's the same as the current table, then use the existing table info
		if currentResultSet != nil && currentResultSet.TableInfo.Name == tableName {
			tableInfo = currentResultSet.TableInfo
		}

		// Otherwise, describe the table
		tableInfo, err = s.tableReadController.tableService.Describe(ctx, tableName)
		if err != nil {
			return nil, errors.Wrapf(err, "cannot describe table '%v'", tableName)
		}
	} else {
		// Table not specified.  Use the existing table, if any
		if currentResultSet == nil {
			return nil, errors.New("no table currently selected")
		}
		tableInfo = currentResultSet.TableInfo
	}

	newResultSet, err := s.tableReadController.tableService.ScanOrQuery(ctx, tableInfo, expr, nil)
	if err != nil {
		return nil, err
	}
	return newResultSet, nil
}

func (sc *ScriptController) CustomKeyCommand(key string) tea.Cmd {
	_, cmd := sc.scriptManager.LookupKeyBinding(key)
	if cmd == nil {
		return nil
	}

	return func() tea.Msg {
		errChan := sc.waitAndPrintScriptError()
		ctx := context.Background()

		if err := cmd.Invoke(ctx, nil, errChan); err != nil {
			return events.Error(err)
		}
		return nil
	}
}

func (sc *ScriptController) Rebind(bindingName string, newKey string) error {
	return sc.scriptManager.RebindKeyBinding(bindingName, newKey)
}

func (sc *ScriptController) LookupBinding(theKey string) string {
	bindingName, _ := sc.scriptManager.LookupKeyBinding(theKey)
	return bindingName
}

func (sc *ScriptController) UnbindKey(key string) {
	sc.scriptManager.UnbindKey(key)
}

func (c *ScriptController) LookupRelatedItems(idx int) (res tea.Msg) {
	rs := c.tableReadController.state.ResultSet()

	relItems, err := c.scriptManager.RelatedItemOfItem(context.Background(), rs, idx)
	if err != nil {
		return events.Error(err)
	} else if len(relItems) == 0 {
		return events.StatusMsg("No related items available")
	}

	return ShowRelatedItemsOverlay{
		Items: relItems,
		OnSelected: func(item relitems.RelatedItem) tea.Msg {
			if item.OnSelect != nil {
				return item.OnSelect()
			}

			return NewJob(c.jobController, "Running query…", func(ctx context.Context) (*models.ResultSet, error) {
				return c.doQuery(ctx, item.Query, scriptmanager.QueryOptions{
					TableName: item.Table,
				})
			}).OnDone(func(rs *models.ResultSet) tea.Msg {
				return c.tableReadController.setResultSetAndFilter(rs, "", true, resultSetUpdateQuery)
			}).Submit()
		},
	}
}
