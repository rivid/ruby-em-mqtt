class EventMachine::MQTT::ServerConnection < EventMachine::MQTT::Connection
  require 'set'
  require 'socket'

  @@clients = Array.new
  @@clients_hash = Hash.new
  @@subscriptions = EventMachine::MQTT::Subscription.new

  attr_accessor :client_id
  attr_accessor :last_packet
  attr_accessor :keep_alive
  attr_accessor :message_id
  attr_accessor :subscriptions

  attr_reader :timer
  attr_reader :logger

  def initialize(logger)
    @logger = logger
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
    elsif state == :connected and packet.class == MQTT::Packet::Unsubscribe
      unsubscribe(packet)
    elsif state == :connected and packet.class == MQTT::Packet::Publish
      publish(packet)
    elsif packet.class == MQTT::Packet::Disconnect
      logger.info("#{client_id} has disconnected")
      disconnect
    else
      # FIXME: deal with other packet types
      raise MQTT::ProtocolException.new(
        "#{client_id} Wasn't expecting packet of type #{packet.class} when in state #{state}"
      )
      disconnect
    end
  end

  def connect(packet)
    # FIXME: check the protocol name and version
    # FIXME: check the client id is between 1 and 23 charcters
    if packet.client_id.nil? or packet.client_id.length < 1 
      @state = :disconnected
      close_connection
    end 
    self.client_id = packet.client_id

    ## FIXME: disconnect old client with the same ID
    unless @@clients_hash[client_id.to_sym].nil?
      @@clients_hash[client_id.to_sym].disconnect
      logger.debug("disconect old client:#{client_id} #{@@clients_hash[client_id.to_sym].object_id}")
    end
    send_packet MQTT::Packet::Connack.new
    @state = :connected
    @@clients << self
    @@clients_hash[client_id.to_sym] = self
    begin
      port, ip = Socket.unpack_sockaddr_in(get_peername)
    rescue
      logger.error {$1}
      disconnect
    end
    #port, ip = Socket.unpack_sockaddr_in(get_sockname)
    logger.info("new client #{client_id} is now connected from #{ip}:#{port}")

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
    logger.info("client #{client_id} disconnected")
    @state = :disconnected
    unless client_id.nil?
      @@clients_hash[client_id.to_sym]
    end
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
    logger.debug("#{client_id} has subscriptions: #{self.subscriptions.to_a}")

    #logger.info("all clients #{@@clients_hash}")
    # FIXME: send subscribe acknowledgement
    @@clients_hash[client_id.to_sym].send_packet(MQTT::Packet::Suback.new({:granted_qos=>0}))
  end

  def unsubscribe(packet)
    packet.topics.each do |topic,qos|
      self.subscriptions.delete(topic)
      @@subscriptions.remove_sub(client_id,topic)
    end

    # FIXME: send unsubscribe acknowledgement
    @@clients_hash[client_id.to_sym].send_packet(MQTT::Packet::Unsubscribe.new({:topics=>packets.topics}))
  end

  def publish(packet)
    #FIXME: topic length check, drop 0 length topic
    #FIXME: topic_wildcard_len_check
    #FIXME: qos check
    #FIXME: Dropped too large PUBLISH 
    #FIXME: Check for topic access 
    return if packet.topic.nil? or packet.topic.empty?
    @@subscriptions.search_sub(packet.topic).each do |client_id|
      @@clients_hash[client_id.to_sym].send_packet(packet) unless @@clients_hash[client_id.to_sym].nil?
    end
  end

end
