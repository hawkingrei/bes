package server

import (
	"context"
	"io"
	"net"

	"github.com/hawkingrei/bes/model/parser"
	pb "github.com/hawkingrei/bes/server/grpc"

	"github.com/pingcap/log"
	"go.uber.org/zap"
	"golang.org/x/exp/slices"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/emptypb"
)

type server struct {
	pb.PublishBuildEventServer
}

func (*server) PublishBuildToolEventStream(stream pb.PublishBuildEvent_PublishBuildToolEventStreamServer) error {
	// Semantically, the protocol requires we ack events in order.
	acks := make([]int, 0)
	var streamID *pb.StreamId
	for {
		log.Info("waiting for event")
		in, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if streamID == nil {
			streamID = in.OrderedBuildEvent.StreamId
		}
		event := in.GetOrderedBuildEvent().Event
		if event != nil {
			event := event.GetEvent()
			var details *anypb.Any
			switch e := event.(type) {
			case *pb.BuildEvent_InvocationAttemptStarted_:
				details = e.InvocationAttemptStarted.Details
			case *pb.BuildEvent_InvocationAttemptFinished_:
				details = e.InvocationAttemptFinished.Details
			case *pb.BuildEvent_BuildEnqueued_:
				details = e.BuildEnqueued.Details
			case *pb.BuildEvent_BuildFinished_:
				details = e.BuildFinished.Details
			case *pb.BuildEvent_ConsoleOutput_:
			case *pb.BuildEvent_ComponentStreamFinished:
			case *pb.BuildEvent_BazelEvent:
			case *pb.BuildEvent_BuildExecutionEvent:
			case *pb.BuildEvent_SourceFetchEvent:
			default:
				log.Error("unknown event type")
			}
			var data parser.BuildEvent
			err := anypb.UnmarshalTo(details, &data, proto.UnmarshalOptions{})
			if err != nil {
				log.Error("failed to unmarshal event", zap.Error(err))
				break
			}
			test := data.GetTestResult()
			if test != nil {
				log.Info("test result", zap.String("result", test.GetStatusDetails()))
			}
		}
		acks = append(acks, int(in.GetOrderedBuildEvent().SequenceNumber))
	}
	slices.Sort(acks)
	for _, ack := range acks {
		rsp := &pb.PublishBuildToolEventStreamResponse{
			StreamId:       streamID,
			SequenceNumber: int64(ack),
		}
		if err := stream.Send(rsp); err != nil {
			log.Warn("Error sending ack stream for invocation", zap.String("InvocationId", streamID.InvocationId), zap.Error(err))
			return err
		}
	}
	return nil
}

func (*server) PublishLifecycleEvent(_ context.Context, req *pb.PublishLifecycleEventRequest) (*emptypb.Empty, error) {
	log.Info("PublishLifecycleEvent")
	event := req.BuildEvent.GetEvent().Event
	var details *anypb.Any
	switch e := event.(type) {
	case *pb.BuildEvent_InvocationAttemptStarted_:
		details = e.InvocationAttemptStarted.Details
	case *pb.BuildEvent_InvocationAttemptFinished_:
		details = e.InvocationAttemptFinished.Details
	case *pb.BuildEvent_BuildEnqueued_:
		details = e.BuildEnqueued.Details
	case *pb.BuildEvent_BuildFinished_:
		details = e.BuildFinished.Details
	case *pb.BuildEvent_ConsoleOutput_,
		*pb.BuildEvent_ComponentStreamFinished,
		*pb.BuildEvent_BazelEvent,
		*pb.BuildEvent_BuildExecutionEvent,
		*pb.BuildEvent_SourceFetchEvent:
		return &emptypb.Empty{}, nil
	default:
		log.Error("unknown event type")
	}
	if details != nil {
		var data parser.BuildEvent
		err := anypb.UnmarshalTo(details, &data, proto.UnmarshalOptions{})
		if err != nil {
			log.Error("failed to unmarshal event", zap.Error(err))
			return &emptypb.Empty{}, err
		}
		test := data.GetTestResult()
		if test != nil {
			log.Info("test result", zap.String("result", test.GetStatusDetails()))
		}
	}
	return &emptypb.Empty{}, nil
}

func Server() {
	listener, err := net.Listen("tcp", ":8080")
	if err != nil {
		panic(err)
	}

	s := grpc.NewServer()
	pb.RegisterPublishBuildEventServer(s, &server{})
	if err := s.Serve(listener); err != nil {
		log.Fatal("failed to serve: %v", zap.Error(err))
	}
}
