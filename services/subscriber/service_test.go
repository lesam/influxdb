package subscriber_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/influxdata/influxdb/coordinator"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxdb/services/subscriber"
)

const testTimeout = 10 * time.Second

type MetaClient struct {
	DatabasesFn          func() []meta.DatabaseInfo
	WaitForDataChangedFn func() chan struct{}
}

func (m MetaClient) Databases() []meta.DatabaseInfo {
	return m.DatabasesFn()
}

func (m MetaClient) WaitForDataChanged() chan struct{} {
	return m.WaitForDataChangedFn()
}

type Subscription struct {
	WritePointsFn func(*coordinator.WritePointsRequest) error
}

func (s Subscription) WritePointsContext(_ context.Context, p *coordinator.WritePointsRequest) error {
	return s.WritePointsFn(p)
}

func TestService_IgnoreNonMatch(t *testing.T) {
	dataChanged := make(chan struct{})
	ms := MetaClient{}
	ms.WaitForDataChangedFn = func() chan struct{} {
		return dataChanged
	}
	ms.DatabasesFn = func() []meta.DatabaseInfo {
		return []meta.DatabaseInfo{
			{
				Name: "db0",
				RetentionPolicies: []meta.RetentionPolicyInfo{
					{
						Name: "rp0",
						Subscriptions: []meta.SubscriptionInfo{
							{Name: "s0", Mode: "ANY", Destinations: []string{"udp://h0:9093", "udp://h1:9093"}},
						},
					},
				},
			},
		}
	}

	prs := make(chan *coordinator.WritePointsRequest, 2)
	urls := make(chan url.URL, 2)
	newPointsWriter := func(u url.URL) (subscriber.PointsWriter, error) {
		sub := Subscription{}
		sub.WritePointsFn = func(p *coordinator.WritePointsRequest) error {
			prs <- p
			return nil
		}
		urls <- u
		return sub, nil
	}

	s := subscriber.NewService(subscriber.NewConfig())
	s.MetaClient = ms
	s.NewPointsWriter = newPointsWriter
	s.Open()
	defer s.Close()

	// Signal that data has changed
	dataChanged <- struct{}{}

	for _, expURLStr := range []string{"udp://h0:9093", "udp://h1:9093"} {
		var u url.URL
		expURL, _ := url.Parse(expURLStr)
		select {
		case u = <-urls:
		case <-time.After(testTimeout):
			t.Fatal("expected urls")
		}
		if expURL.String() != u.String() {
			t.Fatalf("unexpected url: got %s exp %s", u.String(), expURL.String())
		}
	}

	// Write points that don't match any subscription.
	s.Points() <- &coordinator.WritePointsRequest{
		Database:        "db1",
		RetentionPolicy: "rp0",
	}
	s.Points() <- &coordinator.WritePointsRequest{
		Database:        "db0",
		RetentionPolicy: "rp2",
	}

	// Shouldn't get any prs back
	select {
	case pr := <-prs:
		t.Fatalf("unexpected points request %v", pr)
	default:
	}
	close(dataChanged)
}

func TestService_ModeALL(t *testing.T) {
	dataChanged := make(chan struct{})
	ms := MetaClient{}
	ms.WaitForDataChangedFn = func() chan struct{} {
		return dataChanged
	}
	ms.DatabasesFn = func() []meta.DatabaseInfo {
		return []meta.DatabaseInfo{
			{
				Name: "db0",
				RetentionPolicies: []meta.RetentionPolicyInfo{
					{
						Name: "rp0",
						Subscriptions: []meta.SubscriptionInfo{
							{Name: "s0", Mode: "ALL", Destinations: []string{"udp://h0:9093", "udp://h1:9093"}},
						},
					},
				},
			},
		}
	}

	prs := make(chan *coordinator.WritePointsRequest, 2)
	urls := make(chan url.URL, 2)
	newPointsWriter := func(u url.URL) (subscriber.PointsWriter, error) {
		sub := Subscription{}
		sub.WritePointsFn = func(p *coordinator.WritePointsRequest) error {
			prs <- p
			return nil
		}
		urls <- u
		return sub, nil
	}

	s := subscriber.NewService(subscriber.NewConfig())
	s.MetaClient = ms
	s.NewPointsWriter = newPointsWriter
	s.Open()
	defer s.Close()

	// Signal that data has changed
	dataChanged <- struct{}{}

	for _, expURLStr := range []string{"udp://h0:9093", "udp://h1:9093"} {
		var u url.URL
		expURL, _ := url.Parse(expURLStr)
		select {
		case u = <-urls:
		case <-time.After(testTimeout):
			t.Fatal("expected urls")
		}
		if expURL.String() != u.String() {
			t.Fatalf("unexpected url: got %s exp %s", u.String(), expURL.String())
		}
	}

	// Write points that match subscription with mode ALL
	expPR := &coordinator.WritePointsRequest{
		Database:        "db0",
		RetentionPolicy: "rp0",
	}
	s.Points() <- expPR

	// Should get pr back twice
	for i := 0; i < 2; i++ {
		var pr *coordinator.WritePointsRequest
		select {
		case pr = <-prs:
		case <-time.After(testTimeout):
			t.Fatalf("expected points request: got %d exp 2", i)
		}
		if pr != expPR {
			t.Errorf("unexpected points request: got %v, exp %v", pr, expPR)
		}
	}
	close(dataChanged)
}

func TestService_ModeANY(t *testing.T) {
	dataChanged := make(chan struct{})
	ms := MetaClient{}
	ms.WaitForDataChangedFn = func() chan struct{} {
		return dataChanged
	}
	ms.DatabasesFn = func() []meta.DatabaseInfo {
		return []meta.DatabaseInfo{
			{
				Name: "db0",
				RetentionPolicies: []meta.RetentionPolicyInfo{
					{
						Name: "rp0",
						Subscriptions: []meta.SubscriptionInfo{
							{Name: "s0", Mode: "ANY", Destinations: []string{"udp://h0:9093", "udp://h1:9093"}},
						},
					},
				},
			},
		}
	}

	prs := make(chan *coordinator.WritePointsRequest, 2)
	urls := make(chan url.URL, 2)
	newPointsWriter := func(u url.URL) (subscriber.PointsWriter, error) {
		sub := Subscription{}
		sub.WritePointsFn = func(p *coordinator.WritePointsRequest) error {
			prs <- p
			return nil
		}
		urls <- u
		return sub, nil
	}

	s := subscriber.NewService(subscriber.NewConfig())
	s.MetaClient = ms
	s.NewPointsWriter = newPointsWriter
	s.Open()
	defer s.Close()

	// Signal that data has changed
	dataChanged <- struct{}{}

	for _, expURLStr := range []string{"udp://h0:9093", "udp://h1:9093"} {
		var u url.URL
		expURL, _ := url.Parse(expURLStr)
		select {
		case u = <-urls:
		case <-time.After(testTimeout):
			t.Fatal("expected urls")
		}
		if expURL.String() != u.String() {
			t.Fatalf("unexpected url: got %s exp %s", u.String(), expURL.String())
		}
	}
	// Write points that match subscription with mode ANY
	expPR := &coordinator.WritePointsRequest{
		Database:        "db0",
		RetentionPolicy: "rp0",
	}
	s.Points() <- expPR

	// Validate we get the pr back just once
	var pr *coordinator.WritePointsRequest
	select {
	case pr = <-prs:
	case <-time.After(testTimeout):
		t.Fatal("expected points request")
	}
	if pr != expPR {
		t.Errorf("unexpected points request: got %v, exp %v", pr, expPR)
	}

	// shouldn't get it a second time
	select {
	case pr = <-prs:
		t.Fatalf("unexpected points request %v", pr)
	default:
	}
	close(dataChanged)
}

func TestService_Multiple(t *testing.T) {
	dataChanged := make(chan struct{})
	ms := MetaClient{}
	ms.WaitForDataChangedFn = func() chan struct{} {
		return dataChanged
	}
	ms.DatabasesFn = func() []meta.DatabaseInfo {
		return []meta.DatabaseInfo{
			{
				Name: "db0",
				RetentionPolicies: []meta.RetentionPolicyInfo{
					{
						Name: "rp0",
						Subscriptions: []meta.SubscriptionInfo{
							{Name: "s0", Mode: "ANY", Destinations: []string{"udp://h0:9093", "udp://h1:9093"}},
						},
					},
					{
						Name: "rp1",
						Subscriptions: []meta.SubscriptionInfo{
							{Name: "s1", Mode: "ALL", Destinations: []string{"udp://h2:9093", "udp://h3:9093"}},
						},
					},
				},
			},
		}
	}

	prs := make(chan *coordinator.WritePointsRequest, 4)
	urls := make(chan url.URL, 4)
	newPointsWriter := func(u url.URL) (subscriber.PointsWriter, error) {
		sub := Subscription{}
		sub.WritePointsFn = func(p *coordinator.WritePointsRequest) error {
			prs <- p
			return nil
		}
		urls <- u
		return sub, nil
	}

	s := subscriber.NewService(subscriber.NewConfig())
	s.MetaClient = ms
	s.NewPointsWriter = newPointsWriter
	s.Open()
	defer s.Close()

	// Signal that data has changed
	dataChanged <- struct{}{}

	for _, expURLStr := range []string{"udp://h0:9093", "udp://h1:9093", "udp://h2:9093", "udp://h3:9093"} {
		var u url.URL
		expURL, _ := url.Parse(expURLStr)
		select {
		case u = <-urls:
		case <-time.After(testTimeout):
			t.Fatal("expected urls")
		}
		if expURL.String() != u.String() {
			t.Fatalf("unexpected url: got %s exp %s", u.String(), expURL.String())
		}
	}

	// Write points that don't match any subscription.
	s.Points() <- &coordinator.WritePointsRequest{
		Database:        "db1",
		RetentionPolicy: "rp0",
	}
	s.Points() <- &coordinator.WritePointsRequest{
		Database:        "db0",
		RetentionPolicy: "rp2",
	}

	// Write points that match subscription with mode ANY
	expPR := &coordinator.WritePointsRequest{
		Database:        "db0",
		RetentionPolicy: "rp0",
	}
	s.Points() <- expPR

	// Validate we get the pr back just once
	var pr *coordinator.WritePointsRequest
	select {
	case pr = <-prs:
	case <-time.After(testTimeout):
		t.Fatal("expected points request")
	}
	if pr != expPR {
		t.Errorf("unexpected points request: got %v, exp %v", pr, expPR)
	}

	// shouldn't get it a second time
	select {
	case pr = <-prs:
		t.Fatalf("unexpected points request %v", pr)
	default:
	}

	// Write points that match subscription with mode ALL
	expPR = &coordinator.WritePointsRequest{
		Database:        "db0",
		RetentionPolicy: "rp1",
	}
	s.Points() <- expPR

	// Should get pr back twice
	for i := 0; i < 2; i++ {
		select {
		case pr = <-prs:
		case <-time.After(testTimeout):
			t.Fatalf("expected points request: got %d exp 2", i)
		}
		if pr != expPR {
			t.Errorf("unexpected points request: got %v, exp %v", pr, expPR)
		}
	}
	close(dataChanged)
}

func TestService_WaitForDataChanged(t *testing.T) {
	dataChanged := make(chan struct{}, 1)
	defer close(dataChanged)
	ms := MetaClient{}
	ms.WaitForDataChangedFn = func() chan struct{} {
		return dataChanged
	}

	done := make(chan struct{})
	receivedBlocking := make(chan struct{})
	receivedNonBlocking := make(chan struct{})

	blockingServer := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {
		receivedBlocking <- struct{}{}
		<-done
	}))
	nonBlockingServer := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {
		receivedNonBlocking <- struct{}{}
	}))
	defer blockingServer.Close()
	defer nonBlockingServer.Close()
	defer close(done)
	defer close(receivedNonBlocking)
	defer close(receivedBlocking)

	currentDatabaseInfo := []meta.DatabaseInfo{
		{
			Name: "db0",
			RetentionPolicies: []meta.RetentionPolicyInfo{
				{
					Name: "rp0",
					Subscriptions: []meta.SubscriptionInfo{
						{Name: "block", Mode: "ALL", Destinations: []string{blockingServer.URL}},
					},
				},
			},
		},
	}

	calls := make(chan bool, 2)
	ms.DatabasesFn = func() []meta.DatabaseInfo {
		calls <- true
		return currentDatabaseInfo
	}

	s := subscriber.NewService(subscriber.NewConfig())
	s.MetaClient = ms
	// Explicitly closed below for testing
	s.Open()

	// DatabaseInfo should be called once during open
	select {
	case <-calls:
	case <-time.After(testTimeout):
		t.Fatal("expected call")
	}

	select {
	case <-calls:
		t.Fatal("unexpected call")
	case <-time.After(time.Millisecond):
	}

	// send a point, receive it on the server that always blocks (simulating a slow remote server)
	s.Points() <- &coordinator.WritePointsRequest{
		Database:        "db0",
		RetentionPolicy: "rp0",
		Points:          []models.Point{models.MustNewPoint("m0", nil, models.Fields{"f": 1.0}, time.Now())},
	}
	select {
	case <-receivedBlocking:
	case <-time.After(testTimeout):
		t.Fatal("expected blocking server to receive a point")
	}

	// Signal that data has changed - two calls to DatabaseInfo implies we already updated once due to channel size
	currentDatabaseInfo[0].RetentionPolicies[0].Subscriptions[0].Destinations[0] = nonBlockingServer.URL
	currentDatabaseInfo[0].RetentionPolicies[0].Subscriptions[0].Name = "nonblock"
	for i := 0 ; i < 2; i ++ {
		dataChanged <- struct{}{}

		// DatabaseInfo should be called once more after data changed
		select {
		case <-calls:
		case <-time.After(testTimeout):
			t.Fatal("expected call")
		}

		select {
		case <-calls:
			t.Fatal("unexpected call")
		case <-time.After(time.Millisecond):
		}
	}

	// send a point, receive it on the server that never blocks (asserting the change was applied correctly)
	s.Points() <- &coordinator.WritePointsRequest{
		Database:        "db0",
		RetentionPolicy: "rp0",
		Points:          []models.Point{models.MustNewPoint("m0", nil, models.Fields{"f": 1.0}, time.Now())},
	}
	select {
	case <-receivedNonBlocking:
	case <-time.After(testTimeout):
		t.Fatal("expected call to non blocking server")
	}
	select {
	case <-receivedBlocking:
		t.Fatal("expected no call to blocking server")
	case <-time.After(testTimeout):
	}

	//Close service ensure not called
	s.Close()
	dataChanged <- struct{}{}
	select {
	case <-calls:
		t.Fatal("unexpected call")
	case <-time.After(time.Millisecond):
	}

}

func TestService_BadUTF8(t *testing.T) {
	dataChanged := make(chan struct{})
	ms := MetaClient{}
	ms.WaitForDataChangedFn = func() chan struct{} {
		return dataChanged
	}
	ms.DatabasesFn = func() []meta.DatabaseInfo {
		return []meta.DatabaseInfo{
			{
				Name: "db0",
				RetentionPolicies: []meta.RetentionPolicyInfo{
					{
						Name: "rp0",
						Subscriptions: []meta.SubscriptionInfo{
							{Name: "s0", Mode: "ALL", Destinations: []string{"udp://h0:9093", "udp://h1:9093"}},
						},
					},
				},
			},
		}
	}

	prs := make(chan *coordinator.WritePointsRequest, 2)
	urls := make(chan url.URL, 2)
	newPointsWriter := func(u url.URL) (subscriber.PointsWriter, error) {
		sub := Subscription{}
		sub.WritePointsFn = func(p *coordinator.WritePointsRequest) error {
			prs <- p
			return nil
		}
		urls <- u
		return sub, nil
	}

	s := subscriber.NewService(subscriber.NewConfig())
	s.MetaClient = ms
	s.NewPointsWriter = newPointsWriter
	s.Open()
	defer s.Close()

	// Signal that data has changed
	dataChanged <- struct{}{}

	for _, expURLStr := range []string{"udp://h0:9093", "udp://h1:9093"} {
		var u url.URL
		expURL, _ := url.Parse(expURLStr)
		select {
		case u = <-urls:
		case <-time.After(testTimeout):
			t.Fatal("expected urls")
		}
		if expURL.String() != u.String() {
			t.Fatalf("unexpected url: got %s exp %s", u.String(), expURL.String())
		}
	}

	badOne := []byte{255, 112, 114, 111, 99} // A known invalid UTF-8 string
	badTwo := []byte{255, 110, 101, 116}     // A known invalid UTF-8 string
	// bad measurement
	// all good
	// bad field name
	// bad tag name
	// bad tag value
	// all good
	fmtString :=
		`%s,tagname=A fieldname=1.1,stringfield="bathyscape1" 6000000000
		measurementname,tagname=a fieldname=1.1,stringfield="bathyscape5" 6000000001
		measurementname,tagname=A %s=1.2,stringfield="bathyscape2" 6000000002
		measurementname,%s=A fieldname=1.3,stringfield="bathyscape3" 6000000003
		measurementname,tagname=%s fieldname=1.4,stringfield="bathyscape4" 6000000004
		measurementname,tagname=a fieldname=1.6,stringfield="bathyscape5" 6000000006`
	pointString := fmt.Sprintf(fmtString, badOne, badTwo, badOne, badTwo)
	verifyNonUTF8Removal(t, pointString, s, prs, []int{1, 5}, "2 good, 4 bad")

	// All points are bad
	fmtString = `measurementname,tagname=A %s=1.2,stringfield="bathyscape2" 6000000002
				measurementname,tagname=%s fieldname=1.4,stringfield="bathyscape4" 6000000003`
	pointString = fmt.Sprintf(fmtString, badTwo, badOne)
	verifyNonUTF8Removal(t, pointString, s, prs, []int{}, "All 2 bad")

	// First point is bad
	fmtString = `measurementname,tagname=A %s=1.2,stringfield="bathyscape2" 6000000004
				measurementname,tagname=a fieldname=1.1,stringfield="bathyscape5" 6000000005
				measurementname,tagname=b fieldname=1.2,stringfield="bathyscape6" 6000000006`
	pointString = fmt.Sprintf(fmtString, badTwo)
	verifyNonUTF8Removal(t, pointString, s, prs, []int{1, 2}, "First of 3 bad")

	// last point is bad
	fmtString = `measurementname,tagname=a fieldname=1.1,stringfield="bathyscape5" 6000000006
				measurementname,tagname=b %s=1.2,stringfield="bathyscape2" 6000000007`
	pointString = fmt.Sprintf(fmtString, badOne)
	verifyNonUTF8Removal(t, pointString, s, prs, []int{0}, "Last of 2 bad")

	// only point is bad
	fmtString = `measurementname,tagname=b %s=1.2,stringfield="bathyscape2" 6000000007`
	pointString = fmt.Sprintf(fmtString, badOne)
	verifyNonUTF8Removal(t, pointString, s, prs, []int{}, "1 bad, 0 good")

	// last 2 points are bad
	fmtString = `measurementname,tagname=a fieldname=1.1,stringfield="bathyscape5" 6000000006
				measurementname,tagname=b %s=1.2,stringfield="bathyscape2" 6000000007
				%s,tagname=A fieldname=1.1,stringfield="bathyscape1" 6000000008`
	pointString = fmt.Sprintf(fmtString, badTwo, badOne)
	verifyNonUTF8Removal(t, pointString, s, prs, []int{0}, "1 good, 2 bad")

	// first 2 points are bad
	fmtString = `measurementname,tagname=b %s=1.2,stringfield="bathyscape2" 6000000007
				%s,tagname=A fieldname=1.1,stringfield="bathyscape1" 6000000008
				measurementname,tagname=a fieldname=1.1,stringfield="bathyscape5" 6000000009`
	pointString = fmt.Sprintf(fmtString, badTwo, badOne)
	verifyNonUTF8Removal(t, pointString, s, prs, []int{2}, "2 bad, 1 good")

	close(dataChanged)
}

func verifyNonUTF8Removal(t *testing.T, pointString string, s *subscriber.Service, prs chan *coordinator.WritePointsRequest, goodLines []int, trialMessage string) {
	points, err := models.ParsePointsString(pointString)
	if err != nil {
		t.Fatalf("%s: %v", trialMessage, err)
	}
	goodPoints := make([]string, 0, len(goodLines))

	for _, line := range goodLines {
		goodPoints = append(goodPoints, points[line].String())
	}

	// Write points that match subscription with mode ALL
	expPR := &coordinator.WritePointsRequest{
		Database:        "db0",
		RetentionPolicy: "rp0",
		Points:          points,
	}
	s.Points() <- expPR

	// Should get pr back twice
	for i := 0; i < 2; i++ {
		var pr *coordinator.WritePointsRequest
		select {
		case pr = <-prs:
			if len(pr.Points) != len(goodPoints) {
				t.Fatalf("%s expected %d points: got %d for %q", trialMessage, len(goodPoints), len(pr.Points), pointString)
			}
			for i, p := range pr.Points {
				if p.String() != goodPoints[i] {
					t.Fatalf("%s expected %q: got %q for %q", trialMessage, goodPoints[i], p.String(), pointString)
				}
			}
		case <-time.After(testTimeout):
			t.Fatalf("%s expected points request: got %d exp 2 for %q", trialMessage, i, pointString)
		}
	}
}
