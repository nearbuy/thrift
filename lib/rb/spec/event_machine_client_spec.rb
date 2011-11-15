#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.
#

require File.expand_path("#{File.dirname(__FILE__)}/spec_helper")

require 'eventmachine'
require File.expand_path("#{File.dirname(__FILE__)}/../lib/thrift/transport/event_machine_transport")
require File.expand_path("#{File.dirname(__FILE__)}/gen-rb.eventmachine/thrift_spec_types")
require File.expand_path("#{File.dirname(__FILE__)}/gen-rb.eventmachine/nonblocking_service")

class ThriftEventMachineClientSpec < Spec::ExampleGroup
  class Handler
    def initialize
      @queue = Queue.new
    end

    attr_accessor :server

    def greeting(english)
      if english
        SpecEventMachineNamespace::Hello.new
      else
        SpecEventMachineNamespace::Hello.new(:greeting => "Aloha!")
      end
    end

    def block
      @queue.pop
    end

    def unblock(n)
      n.times { @queue.push true }
    end

    def sleep(time)
      Kernel.sleep time
    end

    def shutdown
      @server.shutdown(0, false)
    end
  end

  describe Thrift::EventMachineTransport do
    before(:each) do
      @port = 9913
      handler = Handler.new
      processor = SpecEventMachineNamespace::NonblockingService::Processor.new(handler)

      @transport = Thrift::ServerSocket.new('localhost', @port)
      transport_factory = Thrift::FramedTransportFactory.new
      logger = Logger.new(STDERR)
      logger.level = Logger::WARN
      @server = Thrift::NonblockingServer.new(processor, @transport, transport_factory, nil, 5, logger)
      handler.server = @server
      server_started = false
      @server_thread = Thread.new do |master_thread|
        server_started = true
        @server.serve
      end
      while !server_started
        sleep 0.1 #make sure the server has started before trying to connect
      end
    end

    after(:each) do
      @server.shutdown
      @server_thread.kill
      @transport.close
    end

    it "should handle basic message passing" do
      EM.run do
        client_class = SpecEventMachineNamespace::NonblockingService::Client
        con = Thrift::EventMachineTransport.connect(client_class, 'localhost', @port)
        con.callback do |client|
          testcount = 2
          done = lambda {
            testcount -= 1
            EM.stop_event_loop if testcount == 0
          }
          client.greeting(true).callback do |greeting|
            greeting.should == SpecEventMachineNamespace::Hello.new
            done.call
          end
          client.greeting(false).callback do |greeting|
            greeting.should == SpecEventMachineNamespace::Hello.new(:greeting => 'Aloha!')
            done.call
          end
        end
      end
    end

    it "should process events asynchronously" do
      EM.run do
        client_class = SpecEventMachineNamespace::NonblockingService::Client
        con = Thrift::EventMachineTransport.connect(client_class, 'localhost', @port)
        con.callback do |client|
          testcount = 3
          order = []

          done = lambda do
            testcount -= 1

            if testcount == 0
              order.should == [3,1,2]
              EM.stop_event_loop
            end
          end

          test_sleep = lambda do |secs, index|
            client.sleep(secs).callback { order << index; done.call }
          end

          test_sleep.call(0.3, 1)
          test_sleep.call(0.5, 2)
          test_sleep.call(0.1, 3)
        end
      end
    end

    it "should not return any values for void functions" do
      EM.run do
        client_class = SpecEventMachineNamespace::NonblockingService::Client
        con = Thrift::EventMachineTransport.connect(client_class, 'localhost', @port)
        con.callback do |client|
          client.sleep(0.1).callback do |a|
            a.should == nil
            EM.stop_event_loop
          end
        end
      end
    end

    it "should set oneway functions to success state immediately" do
      tick_count = 0
      # track tick count so we can say if something happened in the same
      # loop iteration
      next_tick_inc = proc do
        tick_count += 1
        EM.next_tick(next_tick_inc)
      end
      EM.run do
        EM.next_tick(next_tick_inc)
        client_class = SpecEventMachineNamespace::NonblockingService::Client
        con = Thrift::EventMachineTransport.connect(client_class, 'localhost', @port)
        con.callback do |client|
          testcount = 2

          done = lambda do
            testcount -= 1
            EM.stop_event_loop if testcount == 0
          end

          unblock_loop_count = tick_count
          client.unblock(1).callback do
            tick_count.should == unblock_loop_count
            done.call
          end

          block_loop_count = tick_count
          client.block.callback do
            tick_count.should > block_loop_count
            done.call
          end
        end
      end
    end

  end
end
