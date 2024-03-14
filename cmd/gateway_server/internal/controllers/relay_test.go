package controllers

// Basic imports
import (
	"errors"
	models2 "pokt_gateway_server/internal/apps_registry/models"
	"pokt_gateway_server/mocks"
	"pokt_gateway_server/pkg/pokt/pokt_v0/models"
	"testing"

	"github.com/stretchr/testify/suite"
	"github.com/valyala/fasthttp"
	"go.uber.org/zap"
)

type RelayTestSuite struct {
	suite.Suite
	mockPocketService    *mocks.PocketService
	mockSessionRegistry  *mocks.SessionRegistryService
	mockRelayController  *RelayController
	mockPoktAppsRegistry *mocks.AppsRegistryService
	context              *fasthttp.RequestCtx
}

func (suite *RelayTestSuite) SetupTest() {
	suite.mockPocketService = new(mocks.PocketService)
	suite.mockPoktAppsRegistry = new(mocks.AppsRegistryService)
	suite.mockSessionRegistry = new(mocks.SessionRegistryService)
	suite.context = &fasthttp.RequestCtx{} // mock the fasthttp.RequestCtx
}

// mock app stake private keys
func mockAppStakePrivateKey() *models.Ed25519Account {

	appStake, _ := models.NewAccount("3fe64039816c44e8872e4ef981725b968422e3d49e95a1eb800707591df30fe374039dbe881dd2744e2e0c469cc2241e1e45f14af6975dd89079d22938377849")
	return appStake

}

// mock send relay request function
func (suite *RelayTestSuite) mockSendRelayRequest() *models.SendRelayRequest {

	chainID, path := getPathSegmented(suite.context.Path()) // get the chainID and path from the request path

	return &models.SendRelayRequest{
		Payload: &models.Payload{
			Data:   string(suite.context.PostBody()),
			Method: string(suite.context.Method()),
			Path:   path,
		},
		Signer: mockAppStakePrivateKey(),
		Chain:  chainID,
	}
}

// test for the HandleRelay function in relay.go file using table driven tests to test different scenarios for the function
func (suite *RelayTestSuite) TestHandleRelay() {

	var testResponse string = "test"

	tests := []struct {
		name             string
		setupMocks       func(*fasthttp.RequestCtx)
		path             string
		expectedStatus   int
		expectedResponse *string
	}{
		{
			name: "EmptyChainID",
			setupMocks: func(ctx *fasthttp.RequestCtx) {
				suite.mockRelayController = NewRelayController(suite.mockPocketService, suite.mockPoktAppsRegistry, suite.mockSessionRegistry, zap.NewNop())
			},
			path:             "/relay/",
			expectedStatus:   fasthttp.StatusBadRequest,
			expectedResponse: nil,
		},
		{
			name: "AppStakeNotProvided",
			setupMocks: func(ctx *fasthttp.RequestCtx) {
				suite.mockPoktAppsRegistry.EXPECT().GetApplicationsByChainId("1234").Return([]*models2.PoktApplicationSigner{}, true)
				suite.mockRelayController = NewRelayController(suite.mockPocketService, suite.mockPoktAppsRegistry, suite.mockSessionRegistry, zap.NewNop())
			},
			path:             "/relay/1234",
			expectedStatus:   fasthttp.StatusBadRequest,
			expectedResponse: nil,
		},
		{
			name: "ErrorDispatchingSession",
			setupMocks: func(ctx *fasthttp.RequestCtx) {

				chainID, _ := getPathSegmented(ctx.Path())
				suite.mockPoktAppsRegistry.EXPECT().GetApplicationsByChainId(chainID).Return([]*models2.PoktApplicationSigner{
					{
						Signer: mockAppStakePrivateKey(),
					},
				}, true)
				suite.mockRelayController = NewRelayController(suite.mockPocketService, suite.mockPoktAppsRegistry, suite.mockSessionRegistry, zap.NewNop())
				suite.mockSessionRegistry.EXPECT().GetSession(&models.GetSessionRequest{
					AppPubKey: mockAppStakePrivateKey().PublicKey,
					Chain:     chainID,
				}).Return(nil, errors.New("error dispatching session"))

			},
			path:             "/relay/1234",
			expectedStatus:   fasthttp.StatusInternalServerError,
			expectedResponse: nil,
		},
		{
			name: "ErrorSendingRelay",
			setupMocks: func(ctx *fasthttp.RequestCtx) {

				chainID, _ := getPathSegmented(ctx.Path())

				suite.mockRelayController = NewRelayController(suite.mockPocketService, suite.mockPoktAppsRegistry, suite.mockSessionRegistry, zap.NewNop())

				suite.mockPoktAppsRegistry.EXPECT().GetApplicationsByChainId(chainID).Return([]*models2.PoktApplicationSigner{
					{
						Signer: mockAppStakePrivateKey(),
					},
				}, true)
				suite.mockSessionRegistry.EXPECT().GetSession(&models.GetSessionRequest{
					AppPubKey: mockAppStakePrivateKey().PublicKey,
					Chain:     chainID,
				}).Return(&models.GetSessionResponse{
					Session: &models.Session{
						Nodes: []*models.Node{
							{
								ServiceUrl: "test",
								PublicKey:  "",
							},
						},
						SessionHeader: &models.SessionHeader{
							SessionHeight: 1,
						},
					},
				}, nil)

				suite.mockPocketService.EXPECT().SendRelay(suite.mockSendRelayRequest()).Return(nil, ErrRelayChannelClosed)

			},
			path:             "/relay/1234",
			expectedStatus:   fasthttp.StatusInternalServerError,
			expectedResponse: nil,
		},
		{
			name: "Success",
			setupMocks: func(ctx *fasthttp.RequestCtx) {

				chainID, _ := getPathSegmented(ctx.Path())

				suite.mockRelayController = NewRelayController(suite.mockPocketService, suite.mockPoktAppsRegistry, suite.mockSessionRegistry, zap.NewNop())

				suite.mockPoktAppsRegistry.EXPECT().GetApplicationsByChainId(chainID).Return([]*models2.PoktApplicationSigner{
					{
						Signer: mockAppStakePrivateKey(),
					},
				}, true)
				suite.mockSessionRegistry.EXPECT().GetSession(&models.GetSessionRequest{
					AppPubKey: mockAppStakePrivateKey().PublicKey,
					Chain:     chainID,
				}).Return(&models.GetSessionResponse{
					Session: &models.Session{
						Nodes: []*models.Node{
							{
								ServiceUrl: "test",
								PublicKey:  "",
							},
						},
						SessionHeader: &models.SessionHeader{
							SessionHeight: 1,
						},
					},
				}, nil)

				suite.mockPocketService.EXPECT().SendRelay(suite.mockSendRelayRequest()).
					Return(&models.SendRelayResponse{
						Response: testResponse,
					}, nil)

			},
			path:             "/relay/1234",
			expectedStatus:   fasthttp.StatusOK,
			expectedResponse: &testResponse,
		},
	}
	for _, test := range tests {
		suite.Run(test.name, func() {

			suite.SetupTest() // reset the test suite

			suite.context.Request.SetBody([]byte("test"))
			suite.context.Request.Header.SetMethod("POST")
			suite.context.Request.SetRequestURI(test.path)

			test.setupMocks(suite.context) // setup the mocks for the test

			suite.mockRelayController.HandleRelay(suite.context)

			suite.Equal(test.expectedStatus, suite.context.Response.StatusCode())

			if test.expectedResponse != nil {
				suite.Equal(*test.expectedResponse, string(suite.context.Response.Body()))
			}

		})
	}
}

// test for concurrentRelay function in relay.go file using table driven tests to test different scenarios for the function
func (suite *RelayTestSuite) TestConcurrentRelay() {

	var testResponse string = "test"

	tests := []struct {
		name             string
		setupMocks       func(*fasthttp.RequestCtx)
		expectedResponse *string
		expectedError    error
	}{
		{
			name: "ErrorSendingRelay",
			setupMocks: func(ctx *fasthttp.RequestCtx) {
				suite.mockPocketService.EXPECT().SendRelay(suite.mockSendRelayRequest()).Return(nil, ErrRelayChannelClosed)
			},
			expectedResponse: nil,
			expectedError:    ErrRelayChannelClosed,
		},
		{
			name: "Success",
			setupMocks: func(ctx *fasthttp.RequestCtx) {

				suite.mockPocketService.EXPECT().SendRelay(suite.mockSendRelayRequest()).
					Return(&models.SendRelayResponse{
						Response: testResponse,
					}, nil)

			},
			expectedResponse: &testResponse,
			expectedError:    nil,
		},
	}
	for _, test := range tests {
		suite.Run(test.name, func() {

			suite.SetupTest() // reset the test suite

			suite.context.Request.SetBody([]byte("test"))
			suite.context.Request.Header.SetMethod("POST")
			suite.context.Request.SetRequestURI("/relay/1234")

			test.setupMocks(suite.context) // setup the mocks for the test

			relayController := NewRelayController(suite.mockPocketService, suite.mockPoktAppsRegistry, suite.mockSessionRegistry, zap.NewNop())

			session := &models.Session{
				Nodes: []*models.Node{
					{
						ServiceUrl: "test",
						PublicKey:  "",
					},
				},
				SessionHeader: &models.SessionHeader{
					SessionHeight: 1,
				},
			}

			response, err := relayController.concurrentRelay(suite.mockSendRelayRequest(), session)

			suite.Equal(test.expectedError, err)

			if test.expectedResponse != nil {
				suite.Equal(*test.expectedResponse, response.Response)
			}

		})
	}
}

// test for the getPathSegmented function in relay.go file using table driven tests to test different scenarios for the function
func (suite *RelayTestSuite) TestGetPathSegmented() {

	tests := []struct {
		name         string
		path         string
		expectedPath string
		expectedRest string
	}{
		{
			name:         "EmptyPath",
			path:         "",
			expectedPath: "",
			expectedRest: "",
		},
		{
			name:         "LessThanTwoSegments",
			path:         "/segment1",
			expectedPath: "",
			expectedRest: "",
		},
		{
			name:         "TwoSegments",
			path:         "/segment1/1234",
			expectedPath: "1234",
			expectedRest: "",
		},
		{
			name:         "MoreThanTwoSegments",
			path:         "/segment1/1234/segment2",
			expectedPath: "1234",
			expectedRest: "/segment2",
		},
	}

	for _, test := range tests {
		suite.Run(test.name, func() {

			path, rest := getPathSegmented([]byte(test.path))

			suite.Equal(test.expectedPath, path)
			suite.Equal(test.expectedRest, rest)

		})
	}

}

func TestRelayTestSuite(t *testing.T) {
	suite.Run(t, new(RelayTestSuite))
}
