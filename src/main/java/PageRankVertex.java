import java.awt.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;


public class PageRankVertex implements Vertex<Double, Double> {
    private static final Double damping = 0.85; // the damping ratio, as in the PageRank paper
    private static final Double tolerance = 1e-4; // the tolerance for converge checking
    
    private int vertexID;
    private Double value;
    private int[] outArray;
    private CyclicBarrier barrier;
	private AtomicInteger haltCount;
	private ArrayList<ArrayBlockingQueue<Double>> blockingQueueArray = new ArrayList<ArrayBlockingQueue<Double>>();
    private Double previousValue;
    private int size;
    private boolean firstStepFlag = true;
    private int outEdges = 0;
	private ConcurrentHashMap valueMap;
	private CyclicBarrier mainBarrier;

    public PageRankVertex(int i, int[] outwardArray, CyclicBarrier barrier, int size, AtomicInteger haltCount, ArrayList<ArrayBlockingQueue<Double>> blockingQueueArray, ConcurrentHashMap valueMap, CyclicBarrier mainBarrier) {
		// TODO Auto-generated constructor stub
    	this.value = (double) (1/(double)size);
    	this.previousValue = this.value;
    	this.vertexID = i;
    	this.outArray = outwardArray;
    	this.barrier = barrier; 	
    	this.mainBarrier = mainBarrier;
    	this.haltCount = haltCount;
    	this.blockingQueueArray = blockingQueueArray;
    	this.size = size;
    	this.valueMap = valueMap;
    	for(int j=0; j<this.outArray.length; j++)
    	{
    		if(outArray[j] == 1)
    		{
    			this.outEdges += 1;
    		}
    	}
	}

	@Override
    public int getVertexID() {
        return this.vertexID;
    }

    @Override
    public Double getValue() {
        return this.value;
    }

    @Override
    public void compute(Collection<Double> messages) {
    	try {
//    		System.out.println("started compute for vertex "+ vertexID);
    		if(firstStepFlag == true)
    		{
    			for(int i=0; i<outArray.length; i++)
    			{
    				if(outArray[i] == 1)
    				{
    					sendMessageTo(i, (double)(value/(double)this.outEdges));
    				}
    			}
    			barrier.await();
    			firstStepFlag = false;
    		}
    		else
    		{
    			barrier.await();
    			this.previousValue = this.value;
    			Double sum = 0.0;
    			while(!blockingQueueArray.get(this.vertexID).isEmpty())
    			{
    				sum += blockingQueueArray.get(this.vertexID).remove();
    			}
    			value = (0.15/(double)size) + damping*sum;
    			barrier.await();
    			for(int i=0; i<outArray.length; i++)
    			{
    				if(outArray[i] == 1)
    				{
    					sendMessageTo(i, (double)(value/(double)this.outEdges));
    				}
    			}
    			voteToHalt();
    			barrier.await();
//    			System.out.println(haltCount);
//    			System.out.println(this.vertexID + " - " + this.value + " halt count " + haltCount);
//    			barrier.await();
    			
//    			barrier.await();
    		}
    		
    		
    		
//    		System.out.println("started thread for vertex "+ vertexID + " value = " + this.value);
//    		this.haltCount.incrementAndGet();    	
					
//			System.out.println(haltCount);
			
    	} catch (InterruptedException | BrokenBarrierException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

    }

    @Override
    public void sendMessageTo(int vertexID, Double message) {
    	this.blockingQueueArray.get(vertexID).add(message);
//    	System.out.println("added to queue in "+ this.vertexID);
    }

    @Override
    public void voteToHalt() {
    	if(Math.abs(this.value - this.previousValue) < tolerance)
    	{
    		haltCount.incrementAndGet();
    	}
    }

    @Override
    public void run() {
    	
    	while(haltCount.get() != size)
    	{
    		haltCount.set(0);
    		compute(this.blockingQueueArray.get(this.vertexID));
    	}
    	
    	
    	valueMap.put(this.vertexID, this.value);
    	
    	try {
			mainBarrier.await();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (BrokenBarrierException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
    }
}
