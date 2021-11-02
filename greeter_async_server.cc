#include "helloworld.pb.h"
#include <cstdio>
#include <grpcpp/impl/codegen/status.h>
#include <grpcpp/server.h>
#include <iostream>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <unistd.h>

#include <grpc/support/log.h>
#include <grpcpp/grpcpp.h>

#ifdef BAZEL_BUILD
#include "examples/protos/helloworld.grpc.pb.h"
#else
#include "helloworld.grpc.pb.h"
#endif

using grpc::Server;
using grpc::ServerAsyncResponseWriter;
using grpc::ServerBuilder;
using grpc::ServerCompletionQueue;
using grpc::ServerContext;
using grpc::Status;
using helloworld::Greeter;
using helloworld::HelloReply;
using helloworld::HelloRequest;

class ServerImpl final {
public:
  ~ServerImpl() {}

  void Shutdown() {
    server_->Shutdown();
    // Always shutdown the completion queue after the server.
    cq_->Shutdown();
  }

  // There is no shutdown handling in this code.
  void Run() {
    std::string server_address("0.0.0.0:50051");

    ServerBuilder builder;
    // Listen on the given address without any authentication mechanism.
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    // Register "service_" as the instance through which we'll communicate with
    // clients. In this case it corresponds to an *asynchronous* service.
    builder.RegisterService(&service_);
    // Get hold of the completion queue used for the asynchronous communication
    // with the gRPC runtime.
    cq_ = builder.AddCompletionQueue();
    // Finally assemble the server.
    server_ = builder.BuildAndStart();
    std::cout << "Server listening on " << server_address << std::endl;

    // Proceed to the server's main loop.
    HandleRpcs();
  }

private:
  static constexpr int CONNECT = 0, READ = 1, WRITE = 2, FINISH = 3, DONE = 4;
  class CallData;
  struct Event {
    Event(CallData *cd, int event) : cd(cd), event(event) {}
    void Process(bool ok) { cd->Proceed(event, ok); };
    CallData *cd;
    int event;
    bool pending = false;
  };

  class CallData {
  public:
    CallData(Greeter::AsyncService *service, ServerCompletionQueue *cq,
             ServerImpl *server)
        : service_(service), cq_(cq), server(server) {
      stream =
          new grpc::ServerAsyncReaderWriter<::helloworld::HelloReply,
                                            ::helloworld::HelloRequest>(&ctx_);
      ctx_.AsyncNotifyWhenDone(&doneevent);
      service_->RequestSayHello(&ctx_, stream, cq_, cq_, &connectevent);
    }
    void Proceed(int status_, bool ok) {
      switch (status_) {
      case CONNECT:
        server->create_new_rpc();
        stream->Read(&request_, &readevent);
        break;
      case READ:
        if (ok) {
          std::cout << request_.name() << std::endl;
          stream->Read(&request_, &readevent);
        } else {
          // ok false means stream writedone in peer, so begin reply
          std::cout << "Read done" << std::endl;

          // send all to queue at one time, cq will handle these msg one by one
          reply("reply1");
          reply("reply2");
          reply("reply3");
          reply("reply4");
          finish();
        }
        break;
      case WRITE:
        writeevent.pending=false;
        if (!ok) {
          _onwriteerror();
        }else{
          _onwrite();
        }
        break;
      case DONE:
        // client disconnect or canceled
        std::cout << "Call canceled: " << ctx_.IsCancelled() << std::endl;
        break;
      default:
        GPR_ASSERT(status_ == FINISH);
        std::cout << "RPC Finish" << std::endl;
        server->remove_finished_rpc(this);
      }
    }

    void reply(std::string msg) {
      std::unique_ptr<HelloReply> replymsg(new HelloReply);
      replymsg->set_message(msg);
      {
        std::lock_guard<std::mutex> lk(mutex_queue);
        send_queue.emplace(std::move(replymsg));
      }

      // invoke in case no tag in cq
      _onwrite();
    }

    void finish() {
      {
        std::lock_guard<std::mutex> lk(mutex_queue);
        send_queue.emplace(nullptr);
      }
      // invoke in case no tag in cq
      _onwrite();
    }

  private:
    void _onwrite() {
      std::lock_guard<std::mutex> lk(mutex_write);
      std::unique_ptr<HelloReply> reply;
      {
        std::lock_guard<std::mutex> lk(mutex_queue);
        if (send_queue.empty() || writeevent.pending || finishevent.pending) {
          return;
        }
        reply = std::move(send_queue.front());
        send_queue.pop();
      }

      if (reply) {
        writeevent.pending = true;
        stream->Write(*reply, &writeevent);
      } else {
        finishevent.pending = true;
        stream->Finish(Status::OK, &finishevent);
      }
    }

    void _onwriteerror() {}
    void _onreaderror() {}

  private:
    Greeter::AsyncService *service_;
    ServerCompletionQueue *cq_;
    ServerContext ctx_;

    grpc::ServerAsyncReaderWriter<::helloworld::HelloReply,
                                  ::helloworld::HelloRequest> *stream;

    HelloRequest request_;
    int serverwrite3times = 0;
    ServerImpl *server;

    std::mutex mutex_write;
    std::mutex mutex_queue;
    std::queue<std::unique_ptr<HelloReply>> send_queue;
    // events
    Event connectevent{this, CONNECT};
    Event readevent{this, READ};
    Event writeevent{this, WRITE};
    Event finishevent{this, FINISH};
    Event doneevent{this, DONE};
  };

private:
  void HandleRpcs() {
    create_new_rpc();
    void *tag; // uniquely identifies a request.
    bool ok;
    while (true) {
      // Next return if there's an event or when Shutdown
      GPR_ASSERT(cq_->Next(&tag, &ok));
      static_cast<Event *>(tag)->Process(ok);
    }
  }

  void remove_finished_rpc(CallData *p) {
    if (calls.find(p) == calls.end()) {
      std::cout << "Not find rpc" << std::endl;
      return;
    }
    calls.erase(p);
  }

  void create_new_rpc() {
    auto ptr = std::make_shared<CallData>(&service_, cq_.get(), this);
    calls.insert({ptr.get(), ptr});
  }

  std::unique_ptr<ServerCompletionQueue> cq_;
  Greeter::AsyncService service_;
  std::unique_ptr<Server> server_;
  std::map<CallData *, std::shared_ptr<CallData>> calls;
};

int main(int argc, char **argv) {
  {
    ServerImpl server;
    std::thread t([&server]() { server.Run(); });
    sleep(6000);
    server.Shutdown();
    t.join();
  }
  return 0;
}
