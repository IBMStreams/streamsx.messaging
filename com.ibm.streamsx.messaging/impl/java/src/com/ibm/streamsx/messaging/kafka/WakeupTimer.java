package com.ibm.streamsx.messaging.kafka;

import java.util.Timer;
import java.util.TimerTask;

/*
 * This class is used to force a wakeup from the poll method because it hangs
 * indefinitely if a connection is lost. 
 */
public class WakeupTimer {
	Timer timer;
	TimerTask task;
	KafkaConsumerClient<?, ?> consumer;
	
	public WakeupTimer(KafkaConsumerClient<?, ?> streamsKafkaConsumer) {
		timer = new Timer();
		this.consumer = streamsKafkaConsumer;
	}
	
	void schedule(long timeInMillis){
		task = new WakeupTimerTask();
		timer.schedule(task, timeInMillis);
	}
	
	void cancelAndPurgeTask(){
		if (task != null){
			task.cancel();
			timer.purge();
			task = null;
		}
	}
	
	public class WakeupTimerTask extends TimerTask {
		@Override
		public void run() {
			System.out.println("Forcing consumer to wakeup.");
			consumer.wakeupConsumer();
		}
	}

	public void close() {
		timer.cancel();
	}
	
	

}
