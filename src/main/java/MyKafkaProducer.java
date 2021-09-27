

import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import twitter4j.FilterQuery;
import twitter4j.Status;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.User;
import twitter4j.conf.ConfigurationBuilder;
import twitter4j.StallWarning;
import twitter4j.StatusDeletionNotice;

public class MyKafkaProducer {
	public static void main(String[] args) throws Exception {

		ConfigurationBuilder cb = new ConfigurationBuilder();
		cb.setDebugEnabled(true)
			.setOAuthConsumerKey("EXeEm1uAM6pcaDdJPajRTwVv0")
			.setOAuthConsumerSecret("s9ra1zPUo6VzsiSpCzTPdX2hJt9rQKVtzFDkkLL8k7VhZhun5h")
			.setOAuthAccessToken("1441588657501052932-KctjziAU9VZwCq72taLEIBglwcS2O5")
			.setOAuthAccessTokenSecret("snSiWxjgfMhT72UOKbIYrCToGaQmsYzC66edWzEbWKXcK");

		MyKafkaProducer producer = new MyKafkaProducer();
		producer.run(cb);
	}
	
	public void run(ConfigurationBuilder cb) {
		final LinkedBlockingQueue<Status> queue = new LinkedBlockingQueue<Status>(1000);

		TwitterStream twitterStreamFactory = new TwitterStreamFactory(cb.build()).getInstance();
		twitterStreamFactory.addListener(new MyKafkaStatusListener(queue));
		
		// Filter keywords
		String[] filter = new String[2];
		filter[0] = "football";
		filter[1] = "ronado";
		twitterStreamFactory.filter(new FilterQuery().track(filter));

		Properties properties = new Properties();
		properties.put("metadata.broker.list", "localhost:9092");
		
		properties.put("bootstrap.servers", "localhost:9092");
		properties.put("acks", "all");
		properties.put("retries", 0);
		properties.put("batch.size", 16384);
		
		properties.put("linger.ms", 1);
		properties.put("buffer.memory", 33554432);
		properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		Producer<String, String> producer = new KafkaProducer<String, String>(properties);
		
		int key = 1;
		while (true) {
			Status data = queue.poll();

			if (data == null) {
				try {
					Thread.sleep(200);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			} else {
				System.out.println(data);
				User user = data.getUser();
				
				producer.send(new ProducerRecord<String, String>("soccerTopic", key++ + "", 
						data.getCreatedAt() + ", " + 
								  user.getName() + ", " + 
								  user.getScreenName() + ", " + 
								  user.getFollowersCount()+ ", " + 
								  user.getFriendsCount() + ", " + 
								  user.getFavouritesCount() + ", " + 
								  data.getLang() + ", " + 
								 "obadah"
								  ));
			}
		}
	}

	public class MyKafkaStatusListener implements StatusListener {

		private final Queue<Status> queue;
		
		public MyKafkaStatusListener(Queue<Status> queue) {
			this.queue = queue;
		}
		
		@Override
		public void onStatus(Status status) {
			queue.offer(status);
		}

		@Override
		public void onException(Exception arg0) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void onDeletionNotice(StatusDeletionNotice arg0) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void onScrubGeo(long arg0, long arg1) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void onStallWarning(StallWarning arg0) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void onTrackLimitationNotice(int arg0) {
			// TODO Auto-generated method stub
			
		}

	}
}
