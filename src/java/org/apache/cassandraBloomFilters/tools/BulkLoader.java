/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandraBloomFilters.tools;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Set;
import javax.net.ssl.SSLContext;

import com.datastax.driver.core.AuthProvider;
import com.datastax.driver.core.JdkSSLOptions;
import com.datastax.driver.core.SSLOptions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import org.apache.cassandraBloomFilters.config.Config;
import org.apache.cassandraBloomFilters.config.DatabaseDescriptor;
import org.apache.cassandraBloomFilters.config.EncryptionOptions;
import org.apache.cassandraBloomFilters.io.sstable.SSTableLoader;
import org.apache.cassandraBloomFilters.security.SSLFactory;
import org.apache.cassandraBloomFilters.streaming.*;
import org.apache.cassandraBloomFilters.utils.JVMStabilityInspector;
import org.apache.cassandraBloomFilters.utils.NativeSSTableLoaderClient;
import org.apache.cassandraBloomFilters.utils.OutputHandler;

public class BulkLoader
{
    public static void main(String args[]) throws BulkLoadException
    {
        LoaderOptions options = LoaderOptions.builder().parseArgs(args).build();
        load(options);
    }

    public static void load(LoaderOptions options) throws BulkLoadException
    {
        Config.setClientMode(true);
        OutputHandler handler = new OutputHandler.SystemOutput(options.verbose, options.debug);
        SSTableLoader loader = new SSTableLoader(
                options.directory,
                new ExternalClient(
                        options.hosts,
                        options.nativePort,
                        options.authProvider,
                        options.storagePort,
                        options.sslStoragePort,
                        options.serverEncOptions,
                        buildSSLOptions((EncryptionOptions.ClientEncryptionOptions)options.encOptions)),
                        handler,
                        options.connectionsPerHost);
        DatabaseDescriptor.setStreamThroughputOutboundMegabitsPerSec(options.throttle);
        DatabaseDescriptor.setInterDCStreamThroughputOutboundMegabitsPerSec(options.interDcThrottle);
        StreamResultFuture future = null;

        ProgressIndicator indicator = new ProgressIndicator();
        try
        {
            if (options.noProgress)
            {
                future = loader.stream(options.ignores);
            }
            else
            {
                future = loader.stream(options.ignores, indicator);
            }

        }
        catch (Exception e)
        {
            JVMStabilityInspector.inspectThrowable(e);
            System.err.println(e.getMessage());
            if (e.getCause() != null)
            {
                System.err.println(e.getCause());
            }
            e.printStackTrace(System.err);
            throw new BulkLoadException(e);
        }

        try
        {
            future.get();

            if (!options.noProgress)
            {
                indicator.printSummary(options.connectionsPerHost);
            }

            // Give sockets time to gracefully close
            Thread.sleep(1000);
            // System.exit(0); // We need that to stop non daemonized threads
        }
        catch (Exception e)
        {
            System.err.println("Streaming to the following hosts failed:");
            System.err.println(loader.getFailedHosts());
            e.printStackTrace(System.err);
            throw new BulkLoadException(e);
        }
    }

    // Return true when everything is at 100%
    static class ProgressIndicator implements StreamEventHandler
    {
        private long start;
        private long lastProgress;
        private long lastTime;

        private int peak = 0;
        private int totalFiles = 0;

        private final Multimap<InetAddress, SessionInfo> sessionsByHost = HashMultimap.create();

        public ProgressIndicator()
        {
            start = lastTime = System.nanoTime();
        }

        public void onSuccess(StreamState finalState)
        {
        }

        public void onFailure(Throwable t)
        {
        }

        public synchronized void handleStreamEvent(StreamEvent event)
        {
            if (event.eventType == StreamEvent.Type.STREAM_PREPARED)
            {
                SessionInfo session = ((StreamEvent.SessionPreparedEvent) event).session;
                sessionsByHost.put(session.peer, session);
            }
            else if (event.eventType == StreamEvent.Type.FILE_PROGRESS || event.eventType == StreamEvent.Type.STREAM_COMPLETE)
            {
                ProgressInfo progressInfo = null;
                if (event.eventType == StreamEvent.Type.FILE_PROGRESS)
                {
                    progressInfo = ((StreamEvent.ProgressEvent) event).progress;
                }

                long time = System.nanoTime();
                long deltaTime = time - lastTime;

                StringBuilder sb = new StringBuilder();
                sb.append("\rprogress: ");

                long totalProgress = 0;
                long totalSize = 0;

                boolean updateTotalFiles = totalFiles == 0;
                // recalculate progress across all sessions in all hosts and display
                for (InetAddress peer : sessionsByHost.keySet())
                {
                    sb.append("[").append(peer).append("]");

                    for (SessionInfo session : sessionsByHost.get(peer))
                    {
                        long size = session.getTotalSizeToSend();
                        long current = 0;
                        int completed = 0;

                        if (progressInfo != null && session.peer.equals(progressInfo.peer) && session.sessionIndex == progressInfo.sessionIndex)
                        {
                            session.updateProgress(progressInfo);
                        }
                        for (ProgressInfo progress : session.getSendingFiles())
                        {
                            if (progress.isCompleted())
                            {
                                completed++;
                            }
                            current += progress.currentBytes;
                        }
                        totalProgress += current;

                        totalSize += size;

                        sb.append(session.sessionIndex).append(":");
                        sb.append(completed).append("/").append(session.getTotalFilesToSend());
                        sb.append(" ").append(String.format("%-3d", size == 0 ? 100L : current * 100L / size)).append("% ");

                        if (updateTotalFiles)
                        {
                            totalFiles += session.getTotalFilesToSend();
                        }
                    }
                }

                lastTime = time;
                long deltaProgress = totalProgress - lastProgress;
                lastProgress = totalProgress;

                sb.append("total: ").append(totalSize == 0 ? 100L : totalProgress * 100L / totalSize).append("% ");
                sb.append(String.format("%-3d", mbPerSec(deltaProgress, deltaTime))).append("MB/s");
                int average = mbPerSec(totalProgress, time - start);
                if (average > peak)
                {
                    peak = average;
                }
                sb.append("(avg: ").append(average).append(" MB/s)");

                System.out.print(sb.toString());
            }
        }

        private int mbPerSec(long bytes, long timeInNano)
        {
            double bytesPerNano = (double)bytes / timeInNano;
            return (int)(bytesPerNano * 1000 * 1000 * 1000 / (1024 * 1024));
        }

        private void printSummary(int connectionsPerHost)
        {
            long end = System.nanoTime();
            long durationMS = (end - start) / 1000000;
            int average = mbPerSec(lastProgress, end - start);
            StringBuilder sb = new StringBuilder();
            sb.append("\nSummary statistics: \n");
            sb.append(String.format("   %-30s: %-10d%n", "Connections per host: ", connectionsPerHost));
            sb.append(String.format("   %-30s: %-10d%n", "Total files transferred: ", totalFiles));
            sb.append(String.format("   %-30s: %-10d%n", "Total bytes transferred: ", lastProgress));
            sb.append(String.format("   %-30s: %-10d%n", "Total duration (ms): ", durationMS));
            sb.append(String.format("   %-30s: %-10d%n", "Average transfer rate (MB/s): ", + average));
            sb.append(String.format("   %-30s: %-10d%n", "Peak transfer rate (MB/s): ", + peak));
            System.out.println(sb.toString());
        }
    }

    private static SSLOptions buildSSLOptions(EncryptionOptions.ClientEncryptionOptions clientEncryptionOptions)
    {

        if (!clientEncryptionOptions.enabled)
        {
            return null;
        }

        SSLContext sslContext;
        try
        {
            sslContext = SSLFactory.createSSLContext(clientEncryptionOptions, true);
        }
        catch (IOException e)
        {
            throw new RuntimeException("Could not create SSL Context.", e);
        }

        return JdkSSLOptions.builder()
                            .withSSLContext(sslContext)
                            .withCipherSuites(clientEncryptionOptions.cipher_suites)
                            .build();
    }

    static class ExternalClient extends NativeSSTableLoaderClient
    {
        private final int storagePort;
        private final int sslStoragePort;
        private final EncryptionOptions.ServerEncryptionOptions serverEncOptions;

        public ExternalClient(Set<InetAddress> hosts,
                              int port,
                              AuthProvider authProvider,
                              int storagePort,
                              int sslStoragePort,
                              EncryptionOptions.ServerEncryptionOptions serverEncryptionOptions,
                              SSLOptions sslOptions)
        {
            super(hosts, port, authProvider, sslOptions);
            this.storagePort = storagePort;
            this.sslStoragePort = sslStoragePort;
            serverEncOptions = serverEncryptionOptions;
        }

        @Override
        public StreamConnectionFactory getConnectionFactory()
        {
            return new BulkLoadConnectionFactory(storagePort, sslStoragePort, serverEncOptions, false);
        }
    }

    public static class CmdLineOptions extends Options
    {
        /**
         * Add option with argument and argument name
         * @param opt shortcut for option name
         * @param longOpt complete option name
         * @param argName argument name
         * @param description description of the option
         * @return updated Options object
         */
        public Options addOption(String opt, String longOpt, String argName, String description)
        {
            Option option = new Option(opt, longOpt, true, description);
            option.setArgName(argName);

            return addOption(option);
        }

        /**
         * Add option without argument
         * @param opt shortcut for option name
         * @param longOpt complete option name
         * @param description description of the option
         * @return updated Options object
         */
        public Options addOption(String opt, String longOpt, String description)
        {
            return addOption(new Option(opt, longOpt, false, description));
        }
    }
}