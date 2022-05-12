public class Count implements Runnable {

    private boolean isRunning = true;

    public void stop() {
        isRunning = false;
    }

    @Override
    public void run() {
        while(isRunning) {
            System.out.println("A");
        }
    }

}
