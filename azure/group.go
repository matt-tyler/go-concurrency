package azure

import (
	"context"
	msgraphsdk "github.com/microsoftgraph/msgraph-sdk-go"
	msgraphcore "github.com/microsoftgraph/msgraph-sdk-go-core"
	"github.com/microsoftgraph/msgraph-sdk-go/models"
)

type GroupProcessor struct {
	client *msgraphsdk.GraphServiceClient
	bufSize int
}

func (p *GroupProcessor) Start(ctx context.Context, _ struct{}) (<-chan models.Groupable, func() error) {
	ch := make(chan models.Groupable, p.bufSize)
	fn := func() error {
		defer close(ch)

		result, err := p.client.Groups().Get(ctx, nil)
		if err != nil {
			return err
		}

		pageIterator, err := msgraphcore.NewPageIterator[models.Groupable](result, p.client.GetAdapter(), models.CreateGroupCollectionResponseFromDiscriminatorValue)
		if err != nil {
			return err
		}

		err = pageIterator.Iterate(context.Background(), func(group models.Groupable) bool {
			ch <- group
    			return true
		})

		return err
	}

	return ch, fn
}

type GroupOwnerProcessor struct {}
func (p *GroupOwnerProcessor) Start(ctx context.Context, group struct{}) <-chan string {
	panic("implement me")
}


