module chainmaker.org/chainmaker/store/performance

go 1.15

require (
	chainmaker.org/chainmaker/localconf/v2 v2.1.1-0.20211214124610-bb7620382194
	chainmaker.org/chainmaker/logger/v2 v2.1.1-0.20211214124250-621f11b35ab0
	chainmaker.org/chainmaker/pb-go/v2 v2.1.1-0.20220308063854-afa02a7d6a4e
	chainmaker.org/chainmaker/protocol/v2 v2.1.2-0.20220310084809-43685565fb18
	chainmaker.org/chainmaker/store/v2 v2.1.1
	chainmaker.org/chainmaker/utils/v2 v2.1.1-0.20220128023017-5bf8279342f1
	github.com/spf13/cobra v1.1.1
)

replace (
	chainmaker.org/chainmaker/store/v2 => ../
	google.golang.org/grpc => google.golang.org/grpc v1.26.0
)
