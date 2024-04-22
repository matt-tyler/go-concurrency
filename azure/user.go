package azure

import (
	"context"
	"time"
	"net/url"
	msgraphsdk "github.com/microsoftgraph/msgraph-sdk-go"
	abs "github.com/microsoft/kiota-abstractions-go"
	msgraphcore "github.com/microsoftgraph/msgraph-sdk-go-core"
	msusers "github.com/microsoftgraph/msgraph-sdk-go/users"
	"github.com/microsoftgraph/msgraph-sdk-go/models"
)

func NewUserProcessor(client *msgraphsdk.GraphServiceClient, bufSize int) *UserProcessor {
	return &UserProcessor{
		client: client,
		bufSize: bufSize,
	}
}

type UserProcessor struct {
	client *msgraphsdk.GraphServiceClient
	bufSize int
}

func (p *UserProcessor) Spawn(ctx context.Context, _ struct{}) (<-chan models.Userable, func() error) {
	ch := make(chan models.Userable, p.bufSize)
	fn := func() error {
		defer close(ch)

		result, err := p.client.Users().Get(ctx, nil)
		if err != nil {
			return err
		}

		pageIterator, err := msgraphcore.NewPageIterator[models.Userable](result, p.client.GetAdapter(), models.CreateUserCollectionResponseFromDiscriminatorValue)
		if err != nil {
			return err
		}

		err = pageIterator.Iterate(context.Background(), func(user models.Userable) bool {
			ch <- user
    			return true
		})

		return err
	}

	return ch, fn
}

func NewUserGroupRequest(ctx context.Context, client *msgraphsdk.GraphServiceClient, id string) (*abs.RequestInformation, error) {
	top := int32(999)
	return client.Users().ByUserId(id).TransitiveMemberOf().ToGetRequestInformation(ctx, &msusers.ItemTransitiveMemberOfRequestBuilderGetRequestConfiguration{
		QueryParameters: &msusers.ItemTransitiveMemberOfRequestBuilderGetQueryParameters{
			Select: []string{"id"},
			Top: &top,
		},
	})
}

type UserGroup struct {
	UserID string
	Group models.Groupable
}

func NewUserGroupProcessor(client *msgraphsdk.GraphServiceClient, batchSize int) *UserGroupProcessor {
	return &UserGroupProcessor{
		client: client,
		batchSize: batchSize,
	}
}

type UserGroupProcessor struct {
	client *msgraphsdk.GraphServiceClient
	batchSize int

}
func (p *UserGroupProcessor) send(ctx context.Context, out chan<-UserGroup, buf []abs.RequestInformation) ([]abs.RequestInformation, error) {
	batch := msgraphcore.NewBatchRequest(p.client.GetAdapter())
	batchSize := min(len(buf), p.batchSize)

	batchItems := make([]msgraphcore.BatchItem, 0, batchSize)
	for i := 0; i < batchSize; i++ {
		batchItem, err := batch.AddBatchRequestStep(buf[i])
		if err != nil {
			return nil, err
		}
		batchItems = append(batchItems, batchItem)
	}

	res, err := batch.Send(ctx, p.client.GetAdapter())
	if err != nil {
		return nil, err
	}

	newBuf := make([]abs.RequestInformation, 0, len(buf))
	_ = copy(newBuf, buf[batchSize:])

	for _, item := range batchItems {
		userID := *item.GetId()
		groups, err := msgraphcore.GetBatchResponseById[models.GroupCollectionResponseable](
			res, userID, models.CreateGroupCollectionResponseFromDiscriminatorValue)
		if err != nil {
			return nil, err
		}

		for _, group := range groups.GetValue() {
			out <- UserGroup{
				UserID: userID,
				Group: group,
			}
		}

		nextLink := groups.GetOdataNextLink()
		if nextLink != nil {
			req := abs.NewRequestInformation()
			targetUrl, err := url.Parse(*nextLink)
			if err != nil {
				return nil, err
			}
			req.SetUri(*targetUrl)
			newBuf = append(newBuf, *req)
		}
	}
	
	return newBuf, nil
}


func (p *UserGroupProcessor) flush(ctx context.Context, out chan<- UserGroup, buf []abs.RequestInformation, force bool) ([]abs.RequestInformation, error) {
	if len(buf) == 0 {
		return buf, nil
	}
	if !force && len(buf) < p.batchSize {
		return buf, nil
	}

	return p.send(ctx, out, buf)
}

func (p *UserGroupProcessor) Spawn(ctx context.Context, users <-chan models.Userable) (<-chan UserGroup, func() error) {
	out := make(chan UserGroup)
	fn := func() (err error) {
		defer close(out)
		buf := make([]abs.RequestInformation, 0, p.batchSize)
		done := ctx.Done()
		timeout := time.NewTimer(1 * time.Second)
		for {
			select {
			case <-done:
				return ctx.Err()
			case user, ok := <-users:
				if !ok {
					return nil
				}
				req, err := NewUserGroupRequest(ctx, p.client, *user.GetId())
				if err != nil {
					return err
				}
				buf = append(buf, *req)
				buf, err = p.flush(ctx, out, buf, false)
				if err != nil {
					return err
				}
			case <-timeout.C:
				buf, err = p.flush(ctx, out, buf, true)
				if err != nil {
					return err
				}
			}
			timeout.Reset(1 * time.Second)
		}
	}
	return out, fn
}
