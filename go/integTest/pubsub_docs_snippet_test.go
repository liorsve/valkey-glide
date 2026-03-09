// Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0

package integTest

import (
	"context"
	"sync"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/valkey-io/valkey-glide/go/v2"
	"github.com/valkey-io/valkey-glide/go/v2/config"
	"github.com/valkey-io/valkey-glide/go/v2/models"
)

func (suite *GlideTestSuite) TestPubSubDocsSnippet_Publish() {
	t := suite.T()
	pub := suite.defaultClient()
	defer pub.Close()

	result, err := pub.Publish(context.Background(), "docs-pub-ch", "Test message")
	assert.NoError(t, err)
	assert.NotNil(t, result)
}

func (suite *GlideTestSuite) TestPubSubDocsSnippet_DynamicSubscribePolling() {
	t := suite.T()
	ch := "docs-poll-" + t.Name()

	pub := suite.defaultClient()
	defer pub.Close()

	sub, err := suite.createAnyClientWithTesting(StandaloneClient, nil)
	require.NoError(t, err)
	defer sub.Close()

	client := sub.(*glide.Client)
	ctx := context.Background()

	// Get queue BEFORE subscribing so it's ready to receive
	queue, err := client.GetQueue()
	require.NoError(t, err)

	err = client.SubscribeLazy(ctx, []string{ch})
	assert.NoError(t, err)
	time.Sleep(1500 * time.Millisecond)

	_, err = pub.Publish(ctx, ch, "hello-poll")
	assert.NoError(t, err)

	select {
	case msg := <-queue.WaitForMessage():
		assert.Equal(t, "hello-poll", msg.Message)
	case <-time.After(5 * time.Second):
		t.Fatal("Timed out waiting for message")
	}
}

func (suite *GlideTestSuite) TestPubSubDocsSnippet_DynamicSubscribeBlocking() {
	t := suite.T()
	ch := "docs-block-" + t.Name()

	pub := suite.defaultClient()
	defer pub.Close()

	sub, err := suite.createAnyClientWithTesting(StandaloneClient, nil)
	require.NoError(t, err)
	defer sub.Close()

	client := sub.(*glide.Client)
	ctx := context.Background()

	// Get queue BEFORE subscribing so it's ready to receive
	queue, err := client.GetQueue()
	require.NoError(t, err)

	err = client.Subscribe(ctx, []string{ch}, 1000)
	assert.NoError(t, err)

	_, err = pub.Publish(ctx, ch, "hello-block")
	assert.NoError(t, err)

	select {
	case msg := <-queue.WaitForMessage():
		assert.Equal(t, "hello-block", msg.Message)
	case <-time.After(5 * time.Second):
		t.Fatal("Timed out waiting for message")
	}
}

func (suite *GlideTestSuite) TestPubSubDocsSnippet_PatternSubscribe() {
	t := suite.T()
	pattern := "docs-pat-*"
	channel := "docs-pat-news"

	pub := suite.defaultClient()
	defer pub.Close()

	sub, err := suite.createAnyClientWithTesting(StandaloneClient, nil)
	require.NoError(t, err)
	defer sub.Close()

	client := sub.(*glide.Client)
	ctx := context.Background()

	// Get queue BEFORE subscribing so it's ready to receive
	queue, err := client.GetQueue()
	require.NoError(t, err)

	err = client.PSubscribeLazy(ctx, []string{pattern})
	assert.NoError(t, err)
	time.Sleep(1500 * time.Millisecond)

	_, err = pub.Publish(ctx, channel, "pattern-msg")
	assert.NoError(t, err)

	select {
	case msg := <-queue.WaitForMessage():
		assert.Equal(t, "pattern-msg", msg.Message)
	case <-time.After(5 * time.Second):
		t.Fatal("Timed out waiting for pattern message")
	}
}

func (suite *GlideTestSuite) TestPubSubDocsSnippet_CallbackBased() {
	t := suite.T()
	ch := "docs-cb-" + t.Name()

	var mu sync.Mutex
	var received []string
	callback := func(message *models.PubSubMessage, ctx any) {
		mu.Lock()
		received = append(received, message.Message)
		mu.Unlock()
	}

	sConfig := config.NewStandaloneSubscriptionConfig().
		WithCallback(callback, nil)
	sub, err := suite.createAnyClientWithTesting(StandaloneClient, sConfig)
	require.NoError(t, err)
	defer sub.Close()

	client := sub.(*glide.Client)
	ctx := context.Background()

	err = client.Subscribe(ctx, []string{ch}, 1000)
	assert.NoError(t, err)

	pub := suite.defaultClient()
	defer pub.Close()

	_, err = pub.Publish(ctx, ch, "cb-hello")
	assert.NoError(t, err)
	time.Sleep(1500 * time.Millisecond)

	mu.Lock()
	assert.NotEmpty(t, received, "Callback should have received at least one message")
	assert.Equal(t, "cb-hello", received[0])
	mu.Unlock()
}

func (suite *GlideTestSuite) TestPubSubDocsSnippet_Unsubscribe() {
	t := suite.T()
	ch := "docs-unsub-" + t.Name()

	sub, err := suite.createAnyClientWithTesting(StandaloneClient, nil)
	require.NoError(t, err)
	defer sub.Close()

	client := sub.(*glide.Client)
	ctx := context.Background()

	err = client.Subscribe(ctx, []string{ch}, 1000)
	assert.NoError(t, err)

	err = client.UnsubscribeLazy(ctx, []string{ch})
	assert.NoError(t, err)
	time.Sleep(1 * time.Second)
}

func (suite *GlideTestSuite) TestPubSubDocsSnippet_StateIntrospection() {
	t := suite.T()
	ch1 := "docs-intr1-" + t.Name()
	ch2 := "docs-intr2-" + t.Name()

	sub, err := suite.createAnyClientWithTesting(StandaloneClient, nil)
	require.NoError(t, err)
	defer sub.Close()

	client := sub.(*glide.Client)
	ctx := context.Background()

	err = client.Subscribe(ctx, []string{ch1, ch2}, 1000)
	assert.NoError(t, err)

	state, err := client.GetSubscriptions(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, state)
}

// Regression test: a client created with NO subscription config should still
// receive messages via polling after dynamic subscribe. Previously the message
// handler was only initialized when a subscription config was provided, causing
// push messages to be silently dropped.
func (suite *GlideTestSuite) TestPubSubDocsSnippet_NoConfigPollingRegression() {
	t := suite.T()
	ch := "docs-noconfig-" + t.Name()

	pub := suite.defaultClient()
	defer pub.Close()

	// Create client with nil subscription config — no callback, no initial channels
	sub, err := suite.createAnyClientWithTesting(StandaloneClient, nil)
	require.NoError(t, err)
	defer sub.Close()

	client := sub.(*glide.Client)
	ctx := context.Background()

	queue, err := client.GetQueue()
	require.NoError(t, err)

	err = client.Subscribe(ctx, []string{ch}, 1000)
	assert.NoError(t, err)

	_, err = pub.Publish(ctx, ch, "no-config-msg")
	assert.NoError(t, err)

	select {
	case msg := <-queue.WaitForMessage():
		assert.Equal(t, "no-config-msg", msg.Message)
		assert.Equal(t, ch, msg.Channel)
	case <-time.After(5 * time.Second):
		t.Fatal("Regression: message not received on client created without subscription config")
	}
}
