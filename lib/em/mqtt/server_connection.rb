require 'set'
class EventMachine::MQTT::ServerConnection < EventMachine::MQTT::Connection

  @@clients = Array.new
  @@clients_hash = Hash.new
  @@subscriptions = Subscription.new

  attr_accessor :client_id
  attr_accessor :last_packet
  attr_accessor :keep_alive
  attr_accessor :message_id
  attr_accessor :subscriptions

  attr_reader :timer
  attr_reader :logger

  def initialize(logger)
    @logger = logger
    @logger.info {'start'}
  end

  def post_init
    super
    @state = :wait_connect
    @client_id = nil
    @keep_alive = 0
    @message_id = 0
    @subscriptions = Set.new
    @timer = nil
    logger.debug("TCP connection opened")
  end

  def unbind
    @@clients.delete(self)
    @@clients_hash.delete(client_id.to_sym)
    @timer.cancel if @timer
    logger.debug("TCP connection closed")
  end

  def process_packet(packet)
    logger.debug("#{client_id}: #{packet.inspect}")

    if state == :wait_connect and packet.class == MQTT::Packet::Connect
      connect(packet)
    elsif state == :connected and packet.class == MQTT::Packet::Pingreq
      ping(packet)
    elsif state == :connected and packet.class == MQTT::Packet::Subscribe
      subscribe(packet)
    elsif state == :connected and packet.class == MQTT::Packet::Publish
      publish(packet)
    elsif packet.class == MQTT::Packet::Disconnect
      logger.info("#{client_id} has disconnected")
      disconnect
    else
      # FIXME: deal with other packet types
      raise MQTT::ProtocolException.new(
        "Wasn't expecting packet of type #{packet.class} when in state #{state}"
      )
      disconnect
    end
  end

  def connect(packet)
    # FIXME: check the protocol name and version
    # FIXME: check the client id is between 1 and 23 charcters
    self.client_id = packet.client_id

    ## FIXME: disconnect old client with the same ID
    if @@clients_hash[client_id.to_sym]
      @@clients_hash[client_id.to_sym].disconnect
    end
    send_packet MQTT::Packet::Connack.new
    @state = :connected
    @@clients << self
    @@clients_hash[client_id.to_sym] = self
    logger.info("#{client_id} is now connected")

    # Setup a keep-alive timer
    if packet.keep_alive
      @keep_alive = packet.keep_alive
      logger.debug("#{client_id}: Setting keep alive timer to #{@keep_alive} seconds")
      @timer = EventMachine::PeriodicTimer.new(@keep_alive / 2) do
        last_seen = Time.now - @last_received
        if last_seen > @keep_alive * 1.5
         logger.info("Disconnecting '#{client_id}' because it hasn't been seen for #{last_seen} seconds")
         disconnect
        end
      end
    end
  end

  def disconnect
    logger.debug("Closing connection to #{client_id}")
    @state = :disconnected
    close_connection
  end

  def ping(packet)
    send_packet MQTT::Packet::Pingresp.new
  end

  def subscribe(packet)
    packet.topics.each do |topic,qos|
      self.subscriptions << topic
      @@subscriptions.add_sub(client_id,topic)
    end
    logger.info("#{client_id} has subscriptions: #{self.subscriptions}")

    # FIXME: send subscribe acknowledgement
  end

  def unsubscribe(packet)
    packet.topics.each do |topic,qos|
      self.subscriptions.delete(topic)
      @@subscriptions.remove_sub(client_id,topic)
    end
    logger.info("#{client_id} has subscriptions: #{self.subscriptions}")

    # FIXME: send subscribe acknowledgement
  end

  def publish(packet)
    #FIXME: topic length check, drop 0 length topic
    #FIXME: topic_wildcard_len_check
    #FIXME: qos check
    #FIXME: Dropped too large PUBLISH 
    #FIXME: Check for topic access 
    @@subscriptions.search(packet.topic).each do |client_id|
      @@clients_hash[client_id.to_sym].send_packet(packet)
    end
  end

end
