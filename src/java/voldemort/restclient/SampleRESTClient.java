/*
 * Copyright 2008-2013 LinkedIn, Inc
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package voldemort.restclient;

import java.util.concurrent.TimeUnit;

import voldemort.client.StoreClient;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;

/**
 * Temporary test until we tie in the Rest Client with the existing StoreClient
 * unit tests.
 * 
 */
public class SampleRESTClient {

    public static void main(String[] args) {

        // Create the client
        RESTClientConfig config = new RESTClientConfig();
        config.setHttpBootstrapURL("http://localhost:8081")
              .setTimeoutMs(1500, TimeUnit.MILLISECONDS)
              .setMaxR2ConnectionPoolSize(100);

        RESTClientFactory factory = new RESTClientFactory(config);
        StoreClient<String, String> clientStore = factory.getStoreClient("test");

        try {
            System.out.println("Value = " + clientStore.get("a"));

            // Sample put
            VectorClock clock = new VectorClock();
            Versioned<String> versionedValue = new Versioned<String>("value", clock);
            System.out.println("Before put : " + clock);
            System.out.println("After put : " + clientStore.put("a", "value"));

            System.out.println("Value = " + clientStore.get("a"));

        } finally {
            factory.close();
        }
    }
}
