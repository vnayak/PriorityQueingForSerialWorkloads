import java.util.PriorityQueue;
import java.util.Random;
import java.util.List;
import java.util.ArrayList;
import java.util.Comparator;


// Title Class
class Title {
    String titleName;
    float priority;
    boolean isIngested;
    boolean isProcessed;
    boolean isDistributed;

    Title(String name, float priority){
        titleName = name;
        isIngested = isProcessed = isDistributed = false;
        this.priority = priority;
    }
}

// Assigns a priority for each title
class TitlePriorityService {

    TitlePriorityService()
    {

    }

    float getRandomPriority() {
        Random r = new Random();
        int low = 10;
        int high = 10000;
        float result = r.nextInt(high-low) + low;
        return result/10000;
    }


}

// Comparator for our priority queue
class TitleComparator implements Comparator<Title>{

    // Overriding compare()method of Comparator
    // for descending order of cgpa
    public int compare(Title s1, Title s2) {
        if (s1.priority < s2.priority)
            return 1;
        else if (s1.priority > s2.priority)
            return -1;
        return 0;
    }
}

// Ingest workload
class Ingestor implements Runnable {
    private Thread t;
    private String threadName;
    List<Title> myTitleList;
    private PriorityQueue<Title> pQueue;

    Ingestor( String name, PriorityQueue<Title> pQ) {
        threadName = name;
        pQueue = pQ;

        System.out.println("Creating " +  threadName );
    }

    private void ingest(Title t){
        //
        t.isIngested = true;
        pQueue.add(t);

    }


    public void run() {
        //System.out.println("Running " +  threadName );

        try {
            System.out.println("Ingesting Titles in queue");
            for(int i = 0; i < Main.NUMTITLES; i++) {
                //System.out.println("Thread: " + threadName + ", " + i);
                Title t = Main.myTitleList.get(i);
                // randomly set a high and low priority for a title to validate if they are processed early or late
                if(i == 50) {
                    t.priority = (float)0.001;
                    System.out.println( "Set a low priority for Title " + t.titleName + " to 0.001" );
                } else if ( i == 10) {
                    t.priority = (float)0.99;
                    System.out.println( "Set a high priority for Title " + t.titleName + " to 0.99" );
                }
                else
                {
                    t.priority = Main.tss.getRandomPriority();
                }
                System.out.println("Ingested Title" + t.titleName + " in queue with priority " + t.priority);
                ingest(t);
                // Let the thread sleep for a while.
                Thread.sleep(10);
            }
            System.out.println("Finished Adding Titles in queue");

        } catch (InterruptedException e) {
            System.out.println("Thread " +  threadName + " interrupted.");
        }
        //printTitles();
        Main.bIngesting = false;
        System.out.println("Thread " +  threadName + " exiting.");
    }

    public void start () {
        System.out.println("Starting " +  threadName );
        if (t == null) {
            t = new Thread (this, threadName);
            t.start ();
        }
    }
}

// Process workload (Ingest->Process->Distribute)
class Processor implements Runnable {
    private Thread t;
    private String threadName;

    private PriorityQueue<Title> pQueue;
    private PriorityQueue<Title> pDistQueue;

    Processor( String name, PriorityQueue<Title> prioQueue, PriorityQueue<Title> prioDQueue) {
        if(prioQueue == null){
            System.out.println("recieved empty queue");
        }
        threadName = name;
        pQueue = prioQueue;
        pDistQueue = prioDQueue;
        Main.bProcessing = true;
        System.out.println("Creating " +  threadName );
    }




    public void run() {
        //System.out.println("Running " +  threadName );

        try {
            System.out.println("Processing Titles in queue");

            while( pQueue.size() > 0 || Main.bIngesting ) {
                if(pQueue.size() == 0) {
                    Thread.sleep(150);
                    System.out.println("Processor: Waiting for more titles to process\n\n");
                    continue;
                }
                Title t = pQueue.poll();
                System.out.println("Processed Title" + t.titleName + " in queue with priority " + t.priority);
                t.isProcessed = true;
                pDistQueue.add(t);
            }
            System.out.println("Finished Processing Titles in queue");


        } catch (InterruptedException e) {
            System.out.println("Thread " +  threadName + " interrupted.");
        }
        Main.bProcessing = false;
        System.out.println("Thread " +  threadName + " exiting.");
    }

    public void start () {
        System.out.println("Starting " +  threadName );
        if (t == null) {
            t = new Thread (this, threadName);
            t.start ();
        }
    }
}

// Distribute workload (Ingest->Process->Distribute)
class Distributor implements Runnable {
    private Thread t;
    private String threadName;

    private PriorityQueue<Title> pQueue;

    Distributor( String name, PriorityQueue<Title> prioQueue) {
        if(prioQueue == null || prioQueue.size() == 0){
            System.out.println("recieved empty queue for Distribution");
        }
        threadName = name;
        pQueue = prioQueue;
        System.out.println("Creating " +  threadName );
    }


    public void run() {
        //System.out.println("Running " +  threadName );

        try {
            System.out.println("Distributing Titles in queue");
            while( pQueue.size() > 0 || Main.bProcessing ) {
                if(pQueue.size() <=0 ) {
                    Thread.sleep(100);
                    System.out.println("Distributor: Waiting for more titles to distribute\n\n");
                    continue;
                }
                Title t = pQueue.poll();
                t.isDistributed = true;

                System.out.println("Distributed Title" + t.titleName + " in queue with priority " + t.priority);
                if(t.isIngested && t.isProcessed && t.isDistributed) {
                    System.out.println("COMPLETED INGN, PROCESSING AND DIST FOR TITLE" + t.titleName + " with priority" + t.priority);
                }
            }
            System.out.println("Finished Distributing Titles in queue");


        } catch (InterruptedException e) {
            System.out.println("Thread " +  threadName + " interrupted.");
        }

        System.out.println("Thread " +  threadName + " exiting.");
    }

    public void start () {
        System.out.println("Starting " +  threadName );
        if (t == null) {
            t = new Thread (this, threadName);
            t.start ();
        }
    }
}

// Main function - initiate title list, Initiate workers for Ingest, Process and Distribute
public class Main {
    public static TitlePriorityService tss = new TitlePriorityService();
    public static int NUMTITLES = 20;
    public static boolean bIngesting = true;
    public static boolean bProcessing = true;
    public static List<Title> myTitleList = new ArrayList<>();

    public static void main(String[] args) {


        PriorityQueue<Title>pProcessingQueue = new PriorityQueue<>(NUMTITLES, new TitleComparator());
        PriorityQueue<Title>pDistributionQueue = new PriorityQueue<>(NUMTITLES, new TitleComparator());

        // Initiate workload of titles
        for (int i = 0; i < Main.NUMTITLES; i++) {
            myTitleList.add(new Title(Integer.toString(i),0));
        }


        for (int i = 0; i < Main.NUMTITLES; i++) {
            Title t = myTitleList.get(i);
            //System.out.println("Created Title " + i +  " " +  t.titleName + "has priority " + t.priority);
        }

        // Start ingest workload
        Ingestor R1 = new Ingestor( "Ingestor", pProcessingQueue);
        R1.start();

        // Start process workload
        Processor P1 = new Processor( "Processor", pProcessingQueue, pDistributionQueue);
        P1.start();

        // Start distribute workload
        Distributor D1 = new Distributor( "Distributor", pDistributionQueue);
        D1.start();

    }
}
