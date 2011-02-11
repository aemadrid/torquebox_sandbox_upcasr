require 'rubygems'
require "bundler/setup"
require 'org.torquebox.torquebox-messaging-client'

class UpcaseService
  def initialize(options={})
    puts "<<< UpcaseService >>> :: initialize : starting ..."
    @halt = false
    @max = options[:max] || 3
    puts "<<< UpcaseService >>> :: initialize : done ..."
  end

  def start
    puts "<<< UpcaseService >>> :: start : starting thread..."
    @queue_threads = (1..@max).each do |idx|
      Thread.new { start_queue idx }
    end
    puts "<<< UpcaseService >>> :: start : done ...."
  end

  def stop
    puts "<<< UpcaseService >>> :: stop : stopping..."
    # Notify our queue receiver to stop
    @halt = true
    # Wait for all spawned threads to exit
    @queue_thread.join
    puts "<<< UpcaseService >>> :: stop : done ..."
  end

  protected

  def start_queue(idx)
    puts "<<< UpcaseService >>> :: start_queue (#{idx}) : sleeping ..."
    sleep 15
    puts "<<< UpcaseService >>> :: start_queue (#{idx}) : starting ..."
    begin
      queue = TorqueBox::Messaging::Queue.new('/queues/upcase')
    rescue Exception => e
      puts "<<< UpcaseService >>> :: start_queue (#{idx}) : exception : #{e}\n#{e.backtrace}"
    end

    while true do
      queue.receive_and_publish(:timeout => 500) do |message|
        result = message.to_s.upcase
        puts "<<< UpcaseService >>> :: start_queue (#{idx}) : Received term [#{message}] and returning [#{result}]"
        result
      end

      # Jump out of the loop if we're shutting down
      if @halt
        puts puts "<<< UpcaseService >>> :: start_queue (#{idx}) : stopping ..."
        break
      end
    end

    puts "<<< UpcaseService >>> :: start_queue (#{idx}) : finished ..."
  end

end