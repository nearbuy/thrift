#!/usr/bin/env ruby

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

$:.push('../gen-rb.eventmachine')
$:.unshift '../../lib/rb/lib'

require 'thrift'
require 'eventmachine'
require 'thrift/transport/event_machine_transport'

require 'calculator'

port = ARGV[0] || 9090
host = "localhost"

EM.run do
  connection = Thrift::EventMachineTransport.connect(Calculator::Client, host, port)
  connection.callback do |client|
    puts "send ping"
    client.ping.callback { puts "ping" }

    puts "send 1+1"
    client.add(1,1).callback {|sum| puts "1+1=#{sum}"}
    puts "send 1+4"
    client.add(1,4).callback {|sum| puts "1+4=#{sum}"}

    work = Work.new()

    work.op = Operation::SUBTRACT
    work.num1 = 15
    work.num2 = 10
    puts "send 15-10"
    client.calculate(1, work).callback {|diff| puts "15-10=#{diff}" }

    puts "send getStruct(1)"
    client.getStruct(1).callback {|log| puts "Log: #{log.value}" }

    work.op = Operation::DIVIDE
    work.num1 = 1
    work.num2 = 0
    puts "send 1/0"
    d = client.calculate(1, work)
    d.callback { puts "Whoa, we can divide by 0 now?" }
    d.errback {|io| puts "InvalidOperation: #{io.why}" }

    puts "send zip\n\n"
    client.zip.callback { puts "zip" }
  end

  connection.errback do
    puts "Could not connect to server #{host}:#{port}"
    EM.stop_event_loop
  end

  EM.add_timer(1) { EM.stop_event_loop }
end
