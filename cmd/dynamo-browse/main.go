package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/lmika/dynamo-browse/internal/common/ui/commandctrl"
	"github.com/lmika/dynamo-browse/internal/common/ui/logging"
	"github.com/lmika/dynamo-browse/internal/common/ui/osstyle"
	"github.com/lmika/dynamo-browse/internal/common/workspaces"
	"github.com/lmika/dynamo-browse/internal/dynamo-browse/controllers"
	"github.com/lmika/dynamo-browse/internal/dynamo-browse/models/queryexpr"
	"github.com/lmika/dynamo-browse/internal/dynamo-browse/providers/dynamo"
	"github.com/lmika/dynamo-browse/internal/dynamo-browse/providers/inputhistorystore"
	"github.com/lmika/dynamo-browse/internal/dynamo-browse/providers/pasteboardprovider"
	"github.com/lmika/dynamo-browse/internal/dynamo-browse/providers/settingstore"
	"github.com/lmika/dynamo-browse/internal/dynamo-browse/providers/workspacestore"
	"github.com/lmika/dynamo-browse/internal/dynamo-browse/services/inputhistory"
	"github.com/lmika/dynamo-browse/internal/dynamo-browse/services/itemrenderer"
	"github.com/lmika/dynamo-browse/internal/dynamo-browse/services/jobs"
	keybindings_service "github.com/lmika/dynamo-browse/internal/dynamo-browse/services/keybindings"
	"github.com/lmika/dynamo-browse/internal/dynamo-browse/services/scriptmanager"
	"github.com/lmika/dynamo-browse/internal/dynamo-browse/services/tables"
	"github.com/lmika/dynamo-browse/internal/dynamo-browse/services/viewsnapshot"
	"github.com/lmika/dynamo-browse/internal/dynamo-browse/ui"
	"github.com/lmika/dynamo-browse/internal/dynamo-browse/ui/keybindings"
	"github.com/lmika/dynamo-browse/internal/dynamo-browse/ui/teamodels/styles"
	bus "github.com/lmika/events"
	"github.com/lmika/gopkgs/cli"
)

func main() {
	var flagTable = flag.String("t", "", "dynamodb table name")
	var flagLocal = flag.String("local", "", "local endpoint")
	var flagDebug = flag.String("debug", "", "file to log debug messages")
	var flagRO = flag.Bool("ro", false, "enable readonly mode")
	var flagDefaultLimit = flag.Int("default-limit", 0, "default limit for queries and scans")
	var flagWorkspace = flag.String("w", "", "workspace file")
	var flagQuery = flag.String("q", "", "run query")
	flag.Parse()

	ctx := context.Background()

	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		cli.Fatalf("cannot load AWS config: %v", err)
	}

	closeFn := logging.EnableLogging(*flagDebug)
	defer closeFn()

	wsManager := workspaces.New(workspaces.MetaInfo{Command: "dynamo-browse"})
	ws, err := wsManager.OpenOrCreate(*flagWorkspace)
	if err != nil {
		cli.Fatalf("cannot create workspace: %v", ws)
	}
	defer ws.Close()

	var dynamoClient *dynamodb.Client
	if *flagLocal != "" {
		host, port, err := net.SplitHostPort(*flagLocal)
		if err != nil {
			cli.Fatalf("invalid address '%v': %v", *flagLocal, err)
		}
		if host == "" {
			host = "localhost"
		}
		if port == "" {
			port = "8000"
		}
		dynamoClient = dynamodb.NewFromConfig(cfg,
			dynamodb.WithEndpointResolver(dynamodb.EndpointResolverFromURL(fmt.Sprintf("http://%v:%v", host, port))))
	} else {
		dynamoClient = dynamodb.NewFromConfig(cfg)
	}

	eventBus := bus.New()

	uiStyles := styles.DefaultStyles
	dynamoProvider := dynamo.NewProvider(dynamoClient)
	resultSetSnapshotStore := workspacestore.NewResultSetSnapshotStore(ws)
	settingStore := settingstore.New(ws)
	inputHistoryStore := inputhistorystore.NewInputHistoryStore(ws)
	pasteboardProvider := pasteboardprovider.New()

	if *flagRO {
		if err := settingStore.SetReadOnly(*flagRO); err != nil {
			cli.Fatalf("unable to set read-only mode: %v", err)
		}
	}
	if *flagDefaultLimit > 0 {
		if err := settingStore.SetDefaultLimit(*flagDefaultLimit); err != nil {
			cli.Fatalf("unable to set default limit: %v", err)
		}
	}

	tableService := tables.NewService(dynamoProvider, settingStore)
	workspaceService := viewsnapshot.NewService(resultSetSnapshotStore)
	itemRendererService := itemrenderer.NewService(uiStyles.ItemView.FieldType, uiStyles.ItemView.MetaInfo)
	scriptManagerService := scriptmanager.New()
	jobsService := jobs.NewService(eventBus)
	inputHistoryService := inputhistory.New(inputHistoryStore)

	state := controllers.NewState()
	jobsController := controllers.NewJobsController(jobsService, eventBus, false)
	tableReadController := controllers.NewTableReadController(
		state,
		tableService,
		workspaceService,
		itemRendererService,
		jobsController,
		inputHistoryService,
		eventBus,
		pasteboardProvider,
		scriptManagerService,
		*flagTable,
	)
	tableWriteController := controllers.NewTableWriteController(state, tableService, jobsController, tableReadController, settingStore)
	columnsController := controllers.NewColumnsController(tableReadController, eventBus)
	exportController := controllers.NewExportController(state, tableService, jobsController, columnsController, pasteboardProvider)
	settingsController := controllers.NewSettingsController(settingStore, eventBus)
	keyBindings := keybindings.Default()
	scriptController := controllers.NewScriptController(scriptManagerService, tableReadController, jobsController, settingsController, eventBus)

	if *flagQuery != "" {
		if *flagTable == "" {
			cli.Fatalf("-t will need to be set for -q")
		}

		ctx := context.Background()

		query, err := queryexpr.Parse(*flagQuery)
		if err != nil {
			cli.Fatalf("query: %v", err)
		}

		ti, err := tableService.Describe(ctx, *flagTable)
		if err != nil {
			cli.Fatalf("cannot describe table: %v", err)
		}

		rs, err := tableService.ScanOrQuery(ctx, ti, query, nil)
		if err != nil {
			cli.Fatalf("cannot execute query: %v", err)
		}
		if err := exportController.ExportToWriter(os.Stdout, rs); err != nil {
			cli.Fatalf("cannot export results of query: %v", err)
		}
		return
	}

	keyBindingService := keybindings_service.NewService(keyBindings)
	keyBindingController := controllers.NewKeyBindingController(keyBindingService, scriptController)

	commandController := commandctrl.NewCommandController(inputHistoryService)
	commandController.AddCommandLookupExtension(scriptController)
	commandController.SetCommandCompletionProvider(columnsController)

	model := ui.NewModel(
		tableReadController,
		tableWriteController,
		columnsController,
		exportController,
		settingsController,
		jobsController,
		itemRendererService,
		commandController,
		scriptController,
		eventBus,
		keyBindingController,
		pasteboardProvider,
		keyBindings,
	)

	// Pre-determine if layout has dark background.  This prevents calls for creating a list to hang.
	osstyle.DetectCurrentScheme()

	p := tea.NewProgram(model, tea.WithAltScreen())

	jobsController.SetMessageSender(p.Send)
	scriptController.Init()
	scriptController.SetMessageSender(p.Send)
	go commandController.StartMessageSender(p.Send)

	log.Println("launching")
	if err := p.Start(); err != nil {
		fmt.Printf("Alas, there's been an error: %v", err)
		os.Exit(1)
	}
}
