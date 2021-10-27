/*
 *
 * Copyright 2015 gRPC authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

#include <grpcpp/impl/codegen/status.h>
#include <grpcpp/server.h>
#include <unistd.h>
#include <cstdio>
#include <iostream>
#include <memory>
#include <string>
#include <thread>

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
  class CallData {
    int CONNECT = 0, READ = 1, WRITE = 2, FINISH = 3, DONE = 4;

   public:
    CallData(Greeter::AsyncService* service, ServerCompletionQueue* cq)
        : service_(service), cq_(cq) {
      stream =
          new grpc::ServerAsyncReaderWriter< ::helloworld::HelloReply,
                                             ::helloworld::HelloRequest>(&ctx_);
      ctx_.AsyncNotifyWhenDone(&DONE);
      service_->RequestSayHello(&ctx_, stream, cq_, cq_, &CONNECT);
    }

    void Proceed(int* status_, bool ok) {
      if (status_ == &CONNECT) {
        stream->Read(&request_, &READ);
        return;
      } else if (status_ == &READ) {
        // ok false means stream writedone in peer, so begin reply
        if (ok) {
          std::cout << request_.name() << std::endl;
          stream->Read(&request_, &READ);
        } else {
          std::cout << "Read done" << std::endl;
          std::string reply("Reply ");
          reply.append(std::to_string(serverwrite3times++));
          reply_.set_message(reply);
          stream->Write(reply_, &WRITE);
        }
        return;
      } else if (status_ == &WRITE) {
        if (serverwrite3times <= 2) {
          std::string reply("Reply ");
          reply.append(std::to_string(serverwrite3times++));
          reply_.set_message(reply);
          stream->Write(reply_, &WRITE);
        } else {
          stream->Finish(Status::OK, &FINISH);
        }
        return;
      } else if (status_ == &DONE) {
		// client disconnect or canceled
        std::cout << "Call canceled: " << ctx_.IsCancelled() <<  std::endl;
        return;
      }
      GPR_ASSERT(status_ == &FINISH);
      std::cout << "Write done" << std::endl;
      delete this;
    }

   private:
    // The means of communication with the gRPC runtime for an asynchronous
    // server.
    Greeter::AsyncService* service_;
    // The producer-consumer queue where for asynchronous server notifications.
    ServerCompletionQueue* cq_;
    // Context for the rpc, allowing to tweak aspects of it such as the use
    // of compression, authentication, as well as to send metadata back to the
    // client.
    ServerContext ctx_;

    grpc::ServerAsyncReaderWriter< ::helloworld::HelloReply,
                                   ::helloworld::HelloRequest>* stream;

    // What we get from the client.
    HelloRequest request_;
    // What we send back to the client.
    HelloReply reply_;
    int serverwrite3times = 0;
  };

  void HandleRpcs() {
    auto onlyone = new CallData(&service_, cq_.get());
    void* tag;  // uniquely identifies a request.
    bool ok;
    while (true) {
		// Next return if there's an event or when Shutdown
      GPR_ASSERT(cq_->Next(&tag, &ok));
      onlyone->Proceed(static_cast<int*>(tag), ok);
    }
  }

  std::unique_ptr<ServerCompletionQueue> cq_;
  Greeter::AsyncService service_;
  std::unique_ptr<Server> server_;
};

int main(int argc, char** argv) {
  {
    ServerImpl server;
    std::thread t([&server]() {
      sleep(60);
      server.Shutdown();
    });
    server.Run();
    t.join();
  }
  return 0;
}
