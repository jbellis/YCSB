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

package com.yahoo.ycsb.measurements;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.yahoo.ycsb.measurements.exporter.MeasurementsExporter;

/**
 * A single measured metric (such as READ LATENCY)
 */
public abstract class OneMeasurement {

	private String _name;
	private final AtomicInteger retrycounts = new AtomicInteger(0);
    
    private final ConcurrentMap<Integer, AtomicInteger> returncodes = new ConcurrentHashMap<Integer, AtomicInteger>();

	public String getName() {
		return _name;
	}

	/**
	 * @param _name
	 */
	public OneMeasurement(String _name) {
        this._name = _name;
    }

	public final void reportReturnCode(int code) {
		AtomicInteger count = returncodes.get(code);
		if (count == null) {
			count = new AtomicInteger();
			AtomicInteger oldCount = returncodes.putIfAbsent(code, count);
			if (oldCount != null) {
				count = oldCount;
			}
		}
		count.incrementAndGet();
	}

	public void reportReturnCodes(MeasurementsExporter exporter)
			throws IOException {
		for (Integer I : returncodes.keySet()) {
			exporter.write(getName(), "Return=" + I, returncodes.get(I).get());
		}
	}
    public final void reportRetryCount(int count) {
        retrycounts.addAndGet(count);
    }
    public int getRetries() {
		return retrycounts.get();
	}

    public abstract void measure(int latency);

    /**
     * This is called periodically by the status thread.
     * 
     * @return summary status for the period since last called
     */
    public abstract String getSummary();

    /**
     * Export the current measurements to a suitable format. This method
     * is called periodically by the ExportMeasurementsThread.
     *
     * @param exporter Exporter representing the type of format to write to.
     * @throws IOException Thrown if the export failed.
     */
    public abstract void exportMeasurementsPart(MeasurementsExporter exporter) throws IOException;
    
    /**
     * Export the current measurements to a suitable format. This method
     * is called periodically by the ExportMeasurementsThread shutdown hook.
     *
     * @param exporter Exporter representing the type of format to write to.
     * @throws IOException Thrown if the export failed.
     */
    public abstract void exportMeasurementsFinal(MeasurementsExporter exporter) throws IOException;
}
