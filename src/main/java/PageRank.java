import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;


public class PageRank {
	private int[][] directionMatrix; //adjacency matrix of vertices. Initialized to all zeros. Set to 1 if vertex of row index has outgoing edge to corresponding column index
	private int size; 	//total number of vertices
	private CyclicBarrier barrier;	//barrier to synchronize vertex operations
//	public CyclicBarrier getBarrier() {
//		return barrier;
//	}
	private CyclicBarrier mainBarrier;

	public CyclicBarrier getMainBarrier() {
		return mainBarrier;
	}

	private AtomicInteger haltCount = new AtomicInteger(0);		//Keeps a count of total number of vertices halted after each superstep
	public AtomicInteger getHaltCount() {
		return haltCount;
	}

	private ArrayList<ArrayBlockingQueue<Double>> blockingQueueArray = new ArrayList<ArrayBlockingQueue<Double>>();		//Each vertex reads from the corresponding index of the blocking queue array at the start of each superstep
	private ConcurrentHashMap<Integer, Double> valueMap;
	
    public PageRank(int size, int[] fromVertices, int[] toVertices) {
    	this.size = size;
    	this.directionMatrix = new int[size][size];
    	this.barrier = new CyclicBarrier(size);
    	this.mainBarrier = new CyclicBarrier(size+1);
    	valueMap = new ConcurrentHashMap<Integer, Double>(size);
//    	ArrayBlockingQueue<Double>[] blockingQueueArray = new ArrayBlockingQueue[size];	
    	for(int i=0; i<size; i++)
    	{
    		for(int j=0; j<size; j++)
    		{
    			this.directionMatrix[i][j] = 0;
    		}
    	}
    	
    	for(int i=0; i<fromVertices.length; i++)
    	{
    		this.directionMatrix[fromVertices[i]][toVertices[i]] = 1;
    	}
    	
    	for(int i=0; i<size; i++)
    	{
    		this.blockingQueueArray.add(new ArrayBlockingQueue<Double>(size));
//    		this.directionMatrix[fromVertices[i]][toVertices[i]] = 1;
    	}
    }

    public void run() throws InterruptedException {
    	for(int i=0; i<size; i++)
    	{
    		PageRankVertex vertex = new PageRankVertex(i, this.directionMatrix[i], this.barrier, this.size, this.haltCount, this.blockingQueueArray, this.valueMap, this.mainBarrier);
    		new Thread(vertex).start();
    	}

    }

    public static void main(String[] args) {
        // Graph has vertices from 0 to `size-1`
        // and edges 1->0, 2->0, 3->0
        int size = 5;
        int[] from = {1,2,3};
        int[] to = {0,0,0};

        PageRank pr = new PageRank(size, from, to);

        try {
            pr.run();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        
        
        try {
			pr.getMainBarrier().await();
		} catch (InterruptedException | BrokenBarrierException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
        
        pr.printValues();
        
        
    }

	private void printValues() {
		// TODO Auto-generated method stub
		Double valueSum = 0.0;
		for(int i=0; i<this.size; i++)
        {
			valueSum += this.valueMap.get(i);
        }
		for(int i=0; i<this.size; i++)
		{
			System.out.println(valueMap.get(i)/valueSum);
		}
		
	}
}
