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

module Thrift
  class EventMachineFramedReader
    def initialize
      @buffer = ''
      @frame_size = -1
      @in_frame = false
    end

    def read_length
      if @buffer.length < 4
        return nil
      end
      @frame_size = @buffer.slice!(0,4).unpack('N').first
      @buffer ||= ''
      @in_frame = true
      return @frame_size
    end

    def read_frame
      frame_buffer = @buffer.slice!(0, @frame_size)
      @in_frame = false
      @frame_size = -1
      @buffer ||= ''
      return Thrift::MemoryBufferTransport.new(frame_buffer)
    end

    def read_frames(data)
      @buffer << data
      if !@in_frame
        read_length
        return if !@in_frame
      end

      frames = []
      while @in_frame && @buffer.length >= @frame_size
        frames << read_frame
        read_length
      end
      return frames
    end
  end

  class EventMachineTransport < EventMachine::Connection
    include EventMachine::Deferrable

    def self.connect(client_class, host, port)
      EM.connect(host, port, self, :client_class => client_class)
    end

    def connection_completed
      @connected = true
      @iprot = Thrift::EventMachineFramedReader.new
      writer = Thrift::FramedTransport.new(self, false)
      @oprot = Thrift::BinaryProtocol.new(writer)

      @client = @client_class.new(@oprot)
      set_deferred_status :succeeded, @client
    end

    def unbind
      if !@connected
        set_deferred_status :failed
      end
    end

    def initialize(args={})
      @client_class = args[:client_class]
      super
    end

    def receive_data(data)
      frames = @iprot.read_frames(data)
      return if frames.empty?

      frames.each do |frame|
        protocol = Thrift::BinaryProtocol.new(frame)
        fname, mtype, rseqid = protocol.read_message_begin
        method_name = 'recv_' + fname
        @client.send(method_name, protocol, mtype, rseqid)
      end
    end

    # transport methods
    def write(data)
      send_data(data)
    end

    def flush
    end
  end
end
