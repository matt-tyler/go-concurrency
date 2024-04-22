package main

import (
	"os"
	"context"
	"example.com/m/runner"
	"example.com/m/jsonl"
	"example.com/m/azure"

	msgraphsdk "github.com/microsoftgraph/msgraph-sdk-go"
	"github.com/microsoftgraph/msgraph-sdk-go/models"
)


func main() {

	ctx := context.Background()

	r := runner.WithContext(ctx)

	var client *msgraphsdk.GraphServiceClient

	userChannel, start := azure.NewUserProcessor(client, 10000).Spawn(ctx, struct{}{})
	r.Go(start)

	userChannelPost := runner.ParallelMap(r, userChannel, 1, jsonl.Encode[models.Userable](os.Stdout))
	
	userGroupProcessor := azure.NewUserGroupProcessor(client, 20)

	userGroupChannel := runner.ParallelChain(r, userChannelPost, 1, userGroupProcessor)

	userGroupChannelPost := runner.ParallelMap(r, userGroupChannel, 1, jsonl.Encode[azure.UserGroup](os.Stdout))
	// _ = runner.ParallelMap(r, userGroupChannel, 1, jsonl.Encode[azure.UserGroup](os.Stdout))

	runner.Consume(r, userGroupChannelPost)
	

	_ = r.Wait()
}
