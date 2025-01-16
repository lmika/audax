module github.com/lmika/dynamo-browse

go 1.23.1

require (
	github.com/alecthomas/participle/v2 v2.1.1
	github.com/asdine/storm v2.1.2+incompatible
	github.com/aws/aws-sdk-go-v2 v1.18.1
	github.com/aws/aws-sdk-go-v2/config v1.18.27
	github.com/aws/aws-sdk-go-v2/credentials v1.13.26
	github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue v1.10.12
	github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression v1.4.39
	github.com/aws/aws-sdk-go-v2/service/dynamodb v1.19.11
	github.com/aws/aws-sdk-go-v2/service/sqs v1.23.2
	github.com/aws/aws-sdk-go-v2/service/ssm v1.24.0
	github.com/brianvoe/gofakeit/v6 v6.15.0
	github.com/calyptia/go-bubble-table v0.2.1
	github.com/charmbracelet/bubbles v0.14.0
	github.com/charmbracelet/bubbletea v0.22.1
	github.com/charmbracelet/lipgloss v0.6.0
	github.com/cloudcmds/tamarin v1.0.0
	github.com/lmika/events v0.0.0-20200906102219-a2269cd4394e
	github.com/lmika/go-bubble-table v0.2.2-0.20220616114432-6bbb2995e538
	github.com/lmika/gopkgs v0.0.0-20240408110817-a02f6fc67d1f
	github.com/lmika/shellwords v0.0.0-20140714114018-ce258dd729fe
	github.com/mattn/go-runewidth v0.0.14
	github.com/muesli/ansi v0.0.0-20211031195517-c9f0611b6c70
	github.com/muesli/reflow v0.3.0
	github.com/pkg/errors v0.9.1
	github.com/stretchr/testify v1.9.0
	golang.design/x/clipboard v0.6.2
	golang.org/x/exp v0.0.0-20230108222341-4b8118a2686a
	golang.org/x/sync v0.1.0
)

require (
	github.com/DataDog/zstd v1.5.2 // indirect
	github.com/Sereal/Sereal v0.0.0-20220220040404-e0d1e550e879 // indirect
	github.com/anthonynsimon/bild v0.13.0 // indirect
	github.com/atotto/clipboard v0.1.4 // indirect
	github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream v1.4.10 // indirect
	github.com/aws/aws-sdk-go-v2/feature/ec2/imds v1.13.4 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.1.34 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.4.28 // indirect
	github.com/aws/aws-sdk-go-v2/internal/ini v1.3.35 // indirect
	github.com/aws/aws-sdk-go-v2/internal/v4a v1.0.26 // indirect
	github.com/aws/aws-sdk-go-v2/service/cloudformation v1.30.0 // indirect
	github.com/aws/aws-sdk-go-v2/service/cloudfront v1.26.8 // indirect
	github.com/aws/aws-sdk-go-v2/service/cloudtrail v1.27.1 // indirect
	github.com/aws/aws-sdk-go-v2/service/cloudwatch v1.26.2 // indirect
	github.com/aws/aws-sdk-go-v2/service/dynamodbstreams v1.14.3 // indirect
	github.com/aws/aws-sdk-go-v2/service/ebs v1.16.14 // indirect
	github.com/aws/aws-sdk-go-v2/service/ec2 v1.102.0 // indirect
	github.com/aws/aws-sdk-go-v2/service/ecr v1.18.13 // indirect
	github.com/aws/aws-sdk-go-v2/service/ecs v1.27.4 // indirect
	github.com/aws/aws-sdk-go-v2/service/eks v1.27.14 // indirect
	github.com/aws/aws-sdk-go-v2/service/elasticache v1.27.2 // indirect
	github.com/aws/aws-sdk-go-v2/service/elasticsearchservice v1.19.2 // indirect
	github.com/aws/aws-sdk-go-v2/service/glue v1.52.0 // indirect
	github.com/aws/aws-sdk-go-v2/service/iam v1.21.0 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.9.11 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/checksum v1.1.29 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/endpoint-discovery v1.7.28 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.9.28 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/s3shared v1.14.3 // indirect
	github.com/aws/aws-sdk-go-v2/service/kinesis v1.17.14 // indirect
	github.com/aws/aws-sdk-go-v2/service/kms v1.22.2 // indirect
	github.com/aws/aws-sdk-go-v2/service/lambda v1.37.0 // indirect
	github.com/aws/aws-sdk-go-v2/service/rds v1.46.0 // indirect
	github.com/aws/aws-sdk-go-v2/service/redshift v1.28.0 // indirect
	github.com/aws/aws-sdk-go-v2/service/route53 v1.28.3 // indirect
	github.com/aws/aws-sdk-go-v2/service/s3 v1.36.0 // indirect
	github.com/aws/aws-sdk-go-v2/service/sfn v1.18.0 // indirect
	github.com/aws/aws-sdk-go-v2/service/sns v1.20.13 // indirect
	github.com/aws/aws-sdk-go-v2/service/sso v1.12.12 // indirect
	github.com/aws/aws-sdk-go-v2/service/ssooidc v1.14.12 // indirect
	github.com/aws/aws-sdk-go-v2/service/sts v1.19.2 // indirect
	github.com/aws/aws-sdk-go-v2/service/wafv2 v1.35.1 // indirect
	github.com/aws/smithy-go v1.13.5 // indirect
	github.com/aymanbagabas/go-osc52 v1.0.3 // indirect
	github.com/containerd/console v1.0.3 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/gofrs/uuid v4.4.0+incompatible // indirect
	github.com/golang/protobuf v1.5.2 // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/hashicorp/errwrap v1.0.0 // indirect
	github.com/hashicorp/go-multierror v1.1.1 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20221227161230-091c0ba34f0a // indirect
	github.com/jackc/pgx/v5 v5.4.1 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/juju/ansiterm v0.0.0-20210929141451-8b71cc96ebdc // indirect
	github.com/lucasb-eyer/go-colorful v1.2.0 // indirect
	github.com/lunixbochs/vtclean v1.0.0 // indirect
	github.com/mattn/go-isatty v0.0.17 // indirect
	github.com/mattn/go-localereader v0.0.1 // indirect
	github.com/muesli/cancelreader v0.2.2 // indirect
	github.com/muesli/termenv v0.13.0 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/risor-io/risor v1.4.0 // indirect
	github.com/rivo/uniseg v0.4.2 // indirect
	github.com/sahilm/fuzzy v0.1.0 // indirect
	github.com/stretchr/objx v0.5.2 // indirect
	github.com/tidwall/gjson v1.14.3 // indirect
	github.com/tidwall/match v1.1.1 // indirect
	github.com/tidwall/pretty v1.2.1 // indirect
	github.com/vmihailenco/msgpack v4.0.4+incompatible // indirect
	github.com/wI2L/jsondiff v0.3.0 // indirect
	go.etcd.io/bbolt v1.3.6 // indirect
	golang.org/x/crypto v0.9.0 // indirect
	golang.org/x/exp/shiny v0.0.0-20230213192124-5e25df0256eb // indirect
	golang.org/x/image v0.5.0 // indirect
	golang.org/x/mobile v0.0.0-20210716004757-34ab1303b554 // indirect
	golang.org/x/sys v0.8.0 // indirect
	golang.org/x/term v0.8.0 // indirect
	golang.org/x/text v0.9.0 // indirect
	google.golang.org/appengine v1.6.7 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
	ucl.lmika.dev v0.0.0-20240501110514-25594c80d273 // indirect
)
