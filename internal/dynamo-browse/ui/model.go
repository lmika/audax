package ui

import (
	"log"
	"os"
	"ucl.lmika.dev/ucl"

	"github.com/charmbracelet/bubbles/key"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/lmika/dynamo-browse/internal/common/ui/commandctrl"
	"github.com/lmika/dynamo-browse/internal/common/ui/events"
	"github.com/lmika/dynamo-browse/internal/dynamo-browse/controllers"
	"github.com/lmika/dynamo-browse/internal/dynamo-browse/models"
	"github.com/lmika/dynamo-browse/internal/dynamo-browse/services"
	"github.com/lmika/dynamo-browse/internal/dynamo-browse/services/itemrenderer"
	"github.com/lmika/dynamo-browse/internal/dynamo-browse/ui/keybindings"
	"github.com/lmika/dynamo-browse/internal/dynamo-browse/ui/teamodels/colselector"
	"github.com/lmika/dynamo-browse/internal/dynamo-browse/ui/teamodels/dialogprompt"
	"github.com/lmika/dynamo-browse/internal/dynamo-browse/ui/teamodels/dynamoitemedit"
	"github.com/lmika/dynamo-browse/internal/dynamo-browse/ui/teamodels/dynamoitemview"
	"github.com/lmika/dynamo-browse/internal/dynamo-browse/ui/teamodels/dynamotableview"
	"github.com/lmika/dynamo-browse/internal/dynamo-browse/ui/teamodels/layout"
	"github.com/lmika/dynamo-browse/internal/dynamo-browse/ui/teamodels/relselector"
	"github.com/lmika/dynamo-browse/internal/dynamo-browse/ui/teamodels/statusandprompt"
	"github.com/lmika/dynamo-browse/internal/dynamo-browse/ui/teamodels/styles"
	"github.com/lmika/dynamo-browse/internal/dynamo-browse/ui/teamodels/tableselect"
	"github.com/lmika/dynamo-browse/internal/dynamo-browse/ui/teamodels/utils"
	bus "github.com/lmika/events"
	"github.com/pkg/errors"
)

const (
	ViewModeTablePrimary   = 0
	ViewModeTableItemEqual = 1
	ViewModeItemPrimary    = 2
	ViewModeItemOnly       = 3
	ViewModeTableOnly      = 4

	ViewModeCount = 5

	initRCFilename = "$HOME/.config/audax/dynamo-browse/init.rc"
)

type Model struct {
	tableReadController  *controllers.TableReadController
	tableWriteController *controllers.TableWriteController
	settingsController   *controllers.SettingsController
	exportController     *controllers.ExportController
	commandController    *commandctrl.CommandController
	scriptController     *controllers.ScriptController
	jobController        *controllers.JobsController
	colSelector          *colselector.Model
	relSelector          *relselector.Model
	itemEdit             *dynamoitemedit.Model
	statusAndPrompt      *statusandprompt.StatusAndPrompt
	tableSelect          *tableselect.Model
	eventBus             *bus.Bus

	mainViewIndex int

	root                 tea.Model
	tableView            *dynamotableview.Model
	itemView             *dynamoitemview.Model
	mainView             tea.Model
	keyMap               *keybindings.ViewKeyBindings
	keyBindingController *controllers.KeyBindingController
}

func NewModel(
	rc *controllers.TableReadController,
	wc *controllers.TableWriteController,
	columnsController *controllers.ColumnsController,
	exportController *controllers.ExportController,
	settingsController *controllers.SettingsController,
	jobController *controllers.JobsController,
	itemRendererService *itemrenderer.Service,
	cc *commandctrl.CommandController,
	scriptController *controllers.ScriptController,
	eventBus *bus.Bus,
	keyBindingController *controllers.KeyBindingController,
	pasteboardProvider services.PasteboardProvider,
	defaultKeyMap *keybindings.KeyBindings,
) Model {
	uiStyles := styles.DefaultStyles

	dtv := dynamotableview.New(defaultKeyMap.TableView, columnsController, settingsController, eventBus, uiStyles)
	div := dynamoitemview.New(itemRendererService, uiStyles)
	mainView := layout.NewVBox(layout.LastChildFixedAt(14), dtv, div)

	colSelector := colselector.New(mainView, defaultKeyMap, columnsController)
	relSelector := relselector.New(colSelector)
	itemEdit := dynamoitemedit.NewModel(relSelector)
	statusAndPrompt := statusandprompt.New(itemEdit, pasteboardProvider, "", uiStyles.StatusAndPrompt)
	dialogPrompt := dialogprompt.New(statusAndPrompt)
	tableSelect := tableselect.New(dialogPrompt, uiStyles)

	cc.AddCommands(&commandctrl.CommandList{
		Commands: map[string]commandctrl.Command{
			"quit": commandctrl.NoArgCommand(tea.Quit),
			"table": func(ctx commandctrl.ExecContext, args ucl.CallArgs) tea.Msg {
				var tableName string
				if err := args.Bind(&tableName); err == nil {
					return rc.ScanTable(tableName)
				}

				return rc.ListTables(false)
			},
			"export": func(ctx commandctrl.ExecContext, args ucl.CallArgs) tea.Msg {
				var filename string
				if err := args.Bind(&filename); err != nil {
					return events.Error(errors.New("expected filename"))
				}

				opts := controllers.ExportOptions{
					AllResults: args.HasSwitch("all"),
				}

				return exportController.ExportCSV(filename, opts)
			},
			"mark": func(ctx commandctrl.ExecContext, args ucl.CallArgs) tea.Msg {
				var markOp = controllers.MarkOpMark

				var markOpStr string
				if err := args.Bind(&markOpStr); err == nil {
					switch markOpStr {
					case "all":
						markOp = controllers.MarkOpMark
					case "none":
						markOp = controllers.MarkOpUnmark
					case "toggle":
						markOp = controllers.MarkOpToggle
					default:
						return events.Error(errors.New("unrecognised mark operation"))
					}
				}

				var whereExpr = ""
				_ = args.BindSwitch("where", &whereExpr)

				return rc.Mark(markOp, whereExpr)
			},
			"unmark": func(ctx commandctrl.ExecContext, args ucl.CallArgs) tea.Msg {
				return rc.Mark(controllers.MarkOpUnmark, "")
			},
			"next-page": func(ctx commandctrl.ExecContext, args ucl.CallArgs) tea.Msg {
				return rc.NextPage()
			},
			"delete": commandctrl.NoArgCommand(wc.DeleteMarked),

			// TEMP
			"new-item": commandctrl.NoArgCommand(wc.NewItem),
			"clone": func(ctx commandctrl.ExecContext, args ucl.CallArgs) tea.Msg {
				return wc.CloneItem(dtv.SelectedItemIndex())
			},
			"set-attr": func(ctx commandctrl.ExecContext, args ucl.CallArgs) tea.Msg {
				var fieldName string
				if err := args.Bind(&fieldName); err != nil {
					return events.Error(errors.New("expected field"))
				}

				var itemType = models.UnsetItemType
				switch {
				case args.HasSwitch("S"):
					itemType = models.StringItemType
				case args.HasSwitch("N"):
					itemType = models.NumberItemType
				case args.HasSwitch("BOOL"):
					itemType = models.BoolItemType
				case args.HasSwitch("NULL"):
					itemType = models.NullItemType
				case args.HasSwitch("TO"):
					itemType = models.ExprValueItemType
				}

				return wc.SetAttributeValue(dtv.SelectedItemIndex(), itemType, fieldName)
			},
			"del-attr": func(ctx commandctrl.ExecContext, args ucl.CallArgs) tea.Msg {
				var fieldName string
				// TODO: support rest args
				if err := args.Bind(&fieldName); err != nil {
					return events.Error(errors.New("expected field"))
				}

				return wc.DeleteAttribute(dtv.SelectedItemIndex(), fieldName)
			},

			"put": func(ctx commandctrl.ExecContext, args ucl.CallArgs) tea.Msg {
				return wc.PutItems()
			},
			"touch": func(ctx commandctrl.ExecContext, args ucl.CallArgs) tea.Msg {
				return wc.TouchItem(dtv.SelectedItemIndex())
			},
			"noisy-touch": func(ctx commandctrl.ExecContext, args ucl.CallArgs) tea.Msg {
				return wc.NoisyTouchItem(dtv.SelectedItemIndex())
			},

			/*
				"echo": func(ctx commandctrl.ExecContext, args ucl.CallArgs) tea.Msg {
					s := new(strings.Builder)
					for _, arg := range args {
						s.WriteString(arg)
					}
					return events.StatusMsg(s.String())
				},
			*/
			"set-opt": func(ctx commandctrl.ExecContext, args ucl.CallArgs) tea.Msg {
				var name string
				if err := args.Bind(&name); err != nil {
					return events.Error(errors.New("expected settingName"))
				}

				var value string
				if err := args.Bind(&value); err == nil {
					return settingsController.SetSetting(name, value)
				}

				return settingsController.SetSetting(name, "")
			},
			"rebind": func(ctx commandctrl.ExecContext, args ucl.CallArgs) tea.Msg {
				var bindingName, newKey string
				if err := args.Bind(&bindingName, &newKey); err != nil {
					return events.Error(errors.New("expected: bindingName newKey"))
				}

				return keyBindingController.Rebind(bindingName, newKey, ctx.FromFile)
			},

			"run-script": func(ctx commandctrl.ExecContext, args ucl.CallArgs) tea.Msg {
				var name string
				if err := args.Bind(&name); err != nil {
					return events.Error(errors.New("expected: script name"))
				}

				return scriptController.RunScript(name)
			},
			"load-script": func(ctx commandctrl.ExecContext, args ucl.CallArgs) tea.Msg {
				var name string
				if err := args.Bind(&name); err != nil {
					return events.Error(errors.New("expected: script name"))
				}

				return scriptController.LoadScript(name)
			},

			// Aliases
			"sa": cc.Alias("set-attr"),
			"da": cc.Alias("del-attr"),
			"np": cc.Alias("next-page"),
			"w":  cc.Alias("put"),
			"q":  cc.Alias("quit"),
		},
	})

	root := layout.FullScreen(tableSelect)

	return Model{
		tableReadController:  rc,
		tableWriteController: wc,
		commandController:    cc,
		scriptController:     scriptController,
		jobController:        jobController,
		itemEdit:             itemEdit,
		colSelector:          colSelector,
		relSelector:          relSelector,
		statusAndPrompt:      statusAndPrompt,
		tableSelect:          tableSelect,
		root:                 root,
		tableView:            dtv,
		itemView:             div,
		mainView:             mainView,
		keyMap:               defaultKeyMap.View,
		keyBindingController: keyBindingController,
	}
}

func (m Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case controllers.SetTableItemView:
		cmd := m.setMainViewIndex(msg.ViewIndex)
		return m, cmd
	case controllers.ResultSetUpdated:
		return m, tea.Batch(
			m.tableView.Refresh(),
			events.SetStatus(msg.StatusMessage()),
		)
	case tea.KeyMsg:
		// TODO: use modes here
		if !m.statusAndPrompt.InPrompt() && !m.tableSelect.Visible() && !m.colSelector.ColSelectorVisible() && !m.relSelector.SelectorVisible() {
			switch {
			case key.Matches(msg, m.keyMap.Mark):
				if idx := m.tableView.SelectedItemIndex(); idx >= 0 {
					return m, events.SetTeaMessage(m.tableWriteController.ToggleMark(idx))
				}
			case key.Matches(msg, m.keyMap.ToggleMarkedItems):
				return m, events.SetTeaMessage(m.tableReadController.Mark(controllers.MarkOpToggle, ""))
			case key.Matches(msg, m.keyMap.CopyItemToClipboard):
				if idx := m.tableView.SelectedItemIndex(); idx >= 0 {
					return m, events.SetTeaMessage(m.tableReadController.CopyItemToClipboard(idx))
				}
			case key.Matches(msg, m.keyMap.CopyTableToClipboard):
				return m, events.SetTeaMessage(m.exportController.ExportCSVToClipboard())
			case key.Matches(msg, m.keyMap.Rescan):
				return m, m.tableReadController.Rescan
			case key.Matches(msg, m.keyMap.PromptForQuery):
				return m, m.tableReadController.PromptForQuery
			case key.Matches(msg, m.keyMap.PromptForFilter):
				return m, m.tableReadController.Filter
			case key.Matches(msg, m.keyMap.FetchNextPage):
				return m, m.tableReadController.NextPage
			case key.Matches(msg, m.keyMap.ViewBack):
				return m, m.tableReadController.ViewBack
			case key.Matches(msg, m.keyMap.ViewForward):
				return m, m.tableReadController.ViewForward
			case key.Matches(msg, m.keyMap.CycleLayoutForward):
				return m, events.SetTeaMessage(controllers.SetTableItemView{ViewIndex: utils.Cycle(m.mainViewIndex, 1, ViewModeCount)})
			case key.Matches(msg, m.keyMap.CycleLayoutBackwards):
				return m, events.SetTeaMessage(controllers.SetTableItemView{ViewIndex: utils.Cycle(m.mainViewIndex, -1, ViewModeCount)})
			//case "e":
			//	m.itemEdit.Visible()
			//	return m, nil
			case key.Matches(msg, m.keyMap.ShowColumnOverlay):
				return m, events.SetTeaMessage(controllers.ShowColumnOverlay{})
			case key.Matches(msg, m.keyMap.ShowRelItemsOverlay):
				if idx := m.tableView.SelectedItemIndex(); idx >= 0 {
					return m, events.SetTeaMessage(m.scriptController.LookupRelatedItems(idx))
				}
			case key.Matches(msg, m.keyMap.PromptForCommand):
				return m, m.commandController.Prompt
			case key.Matches(msg, m.keyMap.PromptForTable):
				return m, events.SetTeaMessage(m.tableReadController.ListTables(false))
			case key.Matches(msg, m.keyMap.CancelRunningJob):
				return m, events.SetTeaMessage(m.jobController.CancelRunningJob(m.promptToQuit))
			case key.Matches(msg, m.keyMap.Quit):
				return m, m.promptToQuit
			default:
				if cmd := m.keyBindingController.LookupCustomBinding(msg.String()); cmd != nil {
					return m, cmd
				}
			}
		}
	}

	var cmd tea.Cmd
	m.root, cmd = m.root.Update(msg)
	return m, cmd
}

func (m Model) Init() tea.Cmd {
	// TODO: this should probably be moved somewhere else
	rcFilename := os.ExpandEnv(initRCFilename)
	if err := m.commandController.ExecuteFile(rcFilename); err != nil {
		log.Println(err)
	}

	return tea.Batch(
		m.tableReadController.Init,
		m.root.Init(),
	)
}

func (m Model) View() string {
	return m.root.View()
}

func (m *Model) setMainViewIndex(viewIndex int) tea.Cmd {
	log.Printf("setting view index = %v", viewIndex)

	var newMainView tea.Model
	switch viewIndex {
	case ViewModeTablePrimary:
		newMainView = layout.NewVBox(layout.LastChildFixedAt(14), m.tableView, m.itemView)
	case ViewModeTableItemEqual:
		newMainView = layout.NewVBox(layout.EqualSize(), m.tableView, m.itemView)
	case ViewModeItemPrimary:
		newMainView = layout.NewVBox(layout.FirstChildFixedAt(7), m.tableView, m.itemView)
	case ViewModeItemOnly:
		newMainView = layout.NewZStack(m.itemView, m.tableView)
	case ViewModeTableOnly:
		newMainView = layout.NewZStack(m.tableView, m.tableView)
	default:
		newMainView = m.mainView
	}

	m.mainViewIndex = viewIndex
	m.mainView = newMainView
	m.itemEdit.SetSubmodel(m.mainView)
	return m.tableView.Refresh()
}

func (m *Model) promptToQuit() tea.Msg {
	return events.Confirm("Quit dynamo-browse? ", func(yes bool) tea.Msg {
		if yes {
			return tea.Quit()
		}
		return nil
	})
}
