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

#include <cstdio>
#include <iostream>
#include <memory>
#include <string>

#include <grpc/support/log.h>
#include <grpcpp/grpcpp.h>

#ifdef BAZEL_BUILD
#include "examples/protos/helloworld.grpc.pb.h"
#else
#include "helloworld.grpc.pb.h"
#endif

using grpc::Channel;
using grpc::ClientAsyncResponseReader;
using grpc::ClientContext;
using grpc::CompletionQueue;
using grpc::Status;
using helloworld::Greeter;
using helloworld::HelloReply;
using helloworld::HelloRequest;

class GreeterClient {
 public:
  explicit GreeterClient(std::shared_ptr<Channel> channel)
      : stub_(Greeter::NewStub(channel)) {}

  // Assembles the client's payload, sends it and presents the response back
  // from the server.
  void Run() {
    // Data we are sending to the server.
    HelloRequest request;

    // Container for the data we expect from the server.
    HelloReply reply;

    // Context for the client. It could be used to convey extra information to
    // the server and/or tweak certain RPC behaviors.
    ClientContext context;

    // The producer-consumer queue we use to communicate asynchronously with the
    // gRPC runtime.
    CompletionQueue cq;

    // Storage for the status of the RPC upon completion.
    Status status;

    auto stream = stub_->PrepareAsyncSayHello(&context, &cq);

    int clientwrite3times = 0;
    int WRITE = 1, READ = 2, WRITEDONE = 3, FINISH = 4;

    stream->StartCall(&WRITE);
    // read and write request can be resisted at the same time
    stream->Read(&reply, &READ);

    void* got_tag;
    bool ok = false;
    // Block until the next result is available in the completion queue "cq".
    // The return value of Next should always be checked. This return value
    // tells us whether there is any kind of event or the cq_ is shutting down.
    while (true) {
      GPR_ASSERT(cq.Next(&got_tag, &ok));

      // Act upon the status of the actual RPC.
      if (!status.ok()) {
        std::cout << "RPC failed" << status.error_message() << std::endl;
      }

      if (got_tag == &WRITE) {
        if (clientwrite3times <= 2) {
          std::string message("Request ");
          message.append(std::to_string(clientwrite3times++));
          request.set_name(message);
          stream->Write(request, &WRITE);
        } else {
          stream->WritesDone(&WRITEDONE);
        }
      } else if (got_tag == &READ) {
        // ok false means stream calls writedone in peer
        if (ok) {
          std::cout << reply.message() << std::endl;
          stream->Read(&reply, &READ);
        } else {
          std::cout << "Read done" << std::endl;
          stream->Finish(&status, &FINISH);
        }
      } else if (got_tag == &WRITEDONE) {
        std::cout << "Write done" << std::endl;
      } else {
        GPR_ASSERT(got_tag == &FINISH);
        std::cout << "Client finish" << std::endl;
        break;
      }
    }
  }

 private:
  // Out of the passed in Channel comes the stub, stored here, our view of the
  // server's exposed services.
  std::unique_ptr<Greeter::Stub> stub_;
};

int main(int argc, char** argv) {
  // Instantiate the client. It requires a channel, out of which the actual RPCs
  // are created. This channel models a connection to an endpoint (in this case,
  // localhost at port 50051). We indicate that the channel isn't authenticated
  // (use of InsecureChannelCredentials()).
  GreeterClient greeter(grpc::CreateChannel(
      "localhost:50051", grpc::InsecureChannelCredentials()));
  greeter.Run();  // The actual RPC call!
  return 0;
}
