package edu.knowitall.browser.entity;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.concurrent.ArrayBlockingQueue;

// a quickly tested but never-used wrapper for a perl script. Runs the script as a separate bash process, feeding it my
// stdin, and forwarding stderr and stdout respectively to mine. 
public class PerlScriptWrapper {

	private static final int queue_size = 1024;
	
	private static final String perls_place = "/usr/local/bin/perl ";
	
	private final ArrayBlockingQueue<String> stdoutQueue = new ArrayBlockingQueue<String>(queue_size);
	private final ArrayBlockingQueue<String> stderrQueue = new ArrayBlockingQueue<String>(queue_size);
	
	private /* mutable */ boolean isFinished = false;
	
	public final String resourceName;
	
	public Process perlProc = null;
	
	public PerlScriptWrapper(String resourceName) { 
		
		this.resourceName = resourceName; 
	}
	
	public void exec() throws IOException, InterruptedException {

		if (isFinished) {
			throw new IllegalStateException("Can't exec me twice!");
		}
		
		// now try to actually fire up perl with raw script as it's argument
		StringBuilder perlCommand = new StringBuilder(perls_place + resourceName);
		// we're going to try to pipe in the script
	
		// Now try to execute this:
		perlProc = Runtime.getRuntime().exec(perlCommand.toString());
		
		new Thread(new Runnable() {

			@Override
			public void run() {
				String line;
				BufferedReader reader = new BufferedReader(new InputStreamReader(perlProc.getInputStream()));
				try {
					while ((line = reader.readLine()) != null) {
						stdoutQueue.put(line);
					}
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
		}).start();
		new Thread(new Runnable() {

			@Override
			public void run() {
				String line;
				BufferedReader reader = new BufferedReader(new InputStreamReader(perlProc.getErrorStream()));
				try {
					while ((line = reader.readLine()) != null) {
						stderrQueue.put(line);
					}
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
		}).start();
		
		synchronized (perlProc) {
			perlProc.wait();
		}
		isFinished = true;
	}
	
	public Iterable<String> getStdout() {
		return stdoutQueue;
	}
	
	public Iterable<String> getStderr() {
		return stderrQueue;
	}
	
	// and now for a main method to test it all out!
	// "/usr/local/bin/perl ./src/main/perl/1-2a2_topcandidates_pc_the.pl"
	
	public static void main(String[] args) throws IOException, InterruptedException {
		
		PerlScriptWrapper ps = new PerlScriptWrapper("./src/main/perl/1-2a2_topcandidates_pc_the.pl");
		
		ps.exec();
		
		System.out.println("Here's what I saw from Stdout:");
		for (String line : ps.getStdout()) { 
			System.out.println(line);
		}
		
		System.out.println();
		System.out.println("Here's what I saw from Stderr:");
		
		for (String line : ps.getStderr()) { 
			System.out.println(line);
		}
	}
}
