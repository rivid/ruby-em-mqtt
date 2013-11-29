class EventMachine::MQTT::Subscription
  attr_reader :topic_node
  def initialize
    @topic_node = Hash.new
  end

  def add_sub client_id, topic
    return if ! validate_sub(topic)
    if @topic_node[topic.to_sym] and (@topic_node[topic.to_sym].is_a? Array)
      @topic_node[topic.to_sym] << client_id.to_sym
    else
      @topic_node[topic.to_sym] = Array.new
      @topic_node[topic.to_sym] << client_id.to_sym
    end
  end

  def remove_sub client_id, topic
    return if ! validate_sub(topic)
    if @topic_node[topic.to_sym] and @topic_node[topic.to_sym].is_a? Array
      @topic_node[topic.to_sym].delete client_id.to_sym
    end
  end

  def search_sub topic
    #return [] if ! validate_sub(topic)
    return [] if topic.include?("#")
    return [] if topic.include?("+")
    part_a = @topic_node[topic.to_sym] || []
    sub_topics = topic.split('/')
    sub_topic = sub_topics[0]
    sub_topics.each_with_index do |t,i|
      if i == 0
        tmp_topic = sub_topic + '/#'
      else
        sub_topic = sub_topic + '/' + t
        tmp_topic = sub_topic + '/#'
      end
      part_a = part_a + (@topic_node[tmp_topic.to_sym] || [])
      tmp_sub_topics = sub_topics.dup
      tmp_sub_topics[i] = '+'
      tmp_topic = tmp_sub_topics.join('/')
      part_a = part_a + (@topic_node[tmp_topic.to_sym] || [])
    end
    part_a
  end

  #FIXME: check topic
  def validate_sub topic
    return false if topic.nil? or topic.empty?
    if topic.include?("#")
      sharp_in_end = (topic.end_with?("#") )
    else
      sharp_in_end = true
    end
    begin
      plus_less_than_one = topic.force_encoding("utf-8").scan("+").count <= 1
    rescue ArgumentError
      puts $!
      puts topic
    end

    sharp_in_end and plus_less_than_one
  end
end


if $0 == __FILE__
  #test performance

  @subs = EventMachine::MQTT::Subscription.new
  @old_clients = Array.new
  class Subs
    attr_accessor :id,:subscriptions
    def initialize id
      @id = id
      @subscriptions = Array.new
    end
  end
  1.upto(100000) do |i|
    id = 'test_'+ i.to_s
    topic = 'topic/' + rand(1000).to_s + '/task'
    @subs.add_sub(id,topic)
    cl = Subs.new(id)
    cl.subscriptions << topic
    @old_clients << cl
  end
    @subs.add_sub("my_id","topic/123/#")
  puts @subs.topic_node.size

  now = Time.new.usec
  clients = @subs.search_sub("topic/123/task")
  puts (Time.new.usec - now.to_i)
  puts clients.size

  now = Time.new.usec
  clients = []
  @old_clients.each do |client|
    topic = ("topic/123/task")
    if client.subscriptions.include?(topic) 
      clients << client.id
    end
  end
  puts (Time.new.usec - now.to_i)
  puts clients.size
end
