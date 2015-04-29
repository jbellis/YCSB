/**                                                                                                                                                                                
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.                                                                                                                             
 *                                                                                                                                                                                 
 * Licensed under the Apache License, Version 2.0 (the "License"); you                                                                                                             
 * may not use this file except in compliance with the License. You                                                                                                                
 * may obtain a copy of the License at                                                                                                                                             
 *                                                                                                                                                                                 
 * http://www.apache.org/licenses/LICENSE-2.0                                                                                                                                      
 *                                                                                                                                                                                 
 * Unless required by applicable law or agreed to in writing, software                                                                                                             
 * distributed under the License is distributed on an "AS IS" BASIS,                                                                                                               
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or                                                                                                                 
 * implied. See the License for the specific language governing                                                                                                                    
 * permissions and limitations under the License. See accompanying                                                                                                                 
 * LICENSE file.                                                                                                                                                                   
 */

package com.yahoo.ycsb;

import static java.util.concurrent.TimeUnit.MICROSECONDS;

import java.util.Random;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Basic DB that just prints out the requested operations, instead of doing them against a database.
 */
public class GoodBadUglyDB extends BasicDB {
    public static final String SIMULATE_DELAY = "gbudb.delays";
    public static final String SIMULATE_DELAY_DEFAULT = "200,1000,10000,50000,100000";
    long delays[];
    static ReadWriteLock DB_ACCESS = new ReentrantReadWriteLock();

    public GoodBadUglyDB() {
        delays = new long[] { 200, 1000, 10000, 50000, 200000 };
    }

    protected void delay() {
        final Random random = Utils.random();
        double p = random.nextDouble();
        int mod;
        if (p < 0.9) {
            mod = 0;
        } else if (p < 0.99) {
            mod = 1;
        } else if (p < 0.9999) {
            mod = 2;
        } else {
            mod = 3;
        }
        // this will make mod 3 pauses global
        Lock lock = mod == 3 ? DB_ACCESS.writeLock() : DB_ACCESS.readLock();
        if (mod == 3) {
            System.out.println("OUCH");
        }
        lock.lock();
        try {
            final long baseDelayNs = MICROSECONDS.toNanos(delays[mod]);
            final int delayRangeNs = (int) (MICROSECONDS.toNanos(delays[mod+1]) - baseDelayNs);
            final long delayNs = baseDelayNs + random.nextInt(delayRangeNs);
            long now = System.nanoTime();
            final long deadline = now + delayNs;
            do {
                LockSupport.parkNanos(deadline - now);
            } while ((now = System.nanoTime()) < deadline && !Thread.interrupted());
        }
        finally {
            lock.unlock();
        }
        
    }

    /**
     * Initialize any state for this DB. Called once per DB instance; there is one DB instance per client thread.
     */
    public void init() {
    	super.init();
        int i=0;
        for(String delay: getProperties().getProperty(SIMULATE_DELAY, SIMULATE_DELAY_DEFAULT).split(",")){
            delays[i++] = Long.parseLong(delay);
        }
    }
}
