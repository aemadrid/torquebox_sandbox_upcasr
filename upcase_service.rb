require 'rubygems'
require "bundler/setup"
require 'org.torquebox.torquebox-messaging-client'

class UpcaseService
  def initialize(options={})
    puts "<<< UpcaseService >>> :: initialize : starting ..."
    @halt = false
    @thread_count = options["thread_count"] || 3
    puts "<<< UpcaseService >>> :: initialize : done ..."
  end

  def start
    puts "<<< UpcaseService >>> :: start : starting threads..."
    @queue_threads = (1..@thread_count).each do |idx|
      puts "<<< UpcaseService >>> :: start : starting thread (#{idx}) ..."
      Thread.new { start_queue idx }
    end
    puts "<<< UpcaseService >>> :: start : done ...."
  end

  def stop
    puts "<<< UpcaseService >>> :: stop : stopping..."
    # Notify our queue receiver to stop
    @halt = true
    # Wait for all spawned threads to exit
    @queue_threads.each { |thread| thread.join }
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
        if message.nil?
          puts "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ <<< UpcaseService >>> :: start_queue (#{idx}) : Received NIL : ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"
          nil
        else
          result = message.to_s.upcase
          puts "<<< UpcaseService >>> :: start_queue (#{idx}) : Received term [#{message}] (#{message.class.name}) and returning [#{result}]"
          result
        end
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