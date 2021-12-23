package com.slack.kaldb.clusterManager;

import static com.slack.kaldb.config.KaldbConfig.DEFAULT_ZK_TIMEOUT_SECS;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.AbstractScheduledService;
import com.slack.kaldb.blobfs.s3.S3BlobFs;
import com.slack.kaldb.metadata.snapshot.SnapshotMetadata;
import com.slack.kaldb.metadata.snapshot.SnapshotMetadataStore;
import com.slack.kaldb.proto.config.KaldbConfigs;
import io.micrometer.core.instrument.MeterRegistry;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ObjectStorageReconciliationService extends AbstractScheduledService {
  private static final Logger LOG =
      LoggerFactory.getLogger(ObjectStorageReconciliationService.class);

  private final KaldbConfigs.ManagerConfig managerConfig;

  private final SnapshotMetadataStore snapshotMetadataStore;
  private final MeterRegistry meterRegistry;
  private final S3BlobFs s3BlobFs;

  @VisibleForTesting protected int futuresListTimeoutSecs = DEFAULT_ZK_TIMEOUT_SECS;

  //  public static final String SNAPSHOT_DELETE_SUCCESS = "snapshot_delete_success";
  //  public static final String SNAPSHOT_DELETE_FAILED = "snapshot_delete_failed";
  //  public static final String SNAPSHOT_DELETE_TIMER = "snapshot_delete_timer";
  //
  //  private final Counter snapshotDeleteSuccess;
  //  private final Counter snapshotDeleteFailed;
  //  private final Timer snapshotDeleteTimer;

  ExecutorService executorService = Executors.newFixedThreadPool(30);

  public ObjectStorageReconciliationService(
      KaldbConfigs.ManagerConfig managerConfig,
      SnapshotMetadataStore snapshotMetadataStore,
      MeterRegistry meterRegistry,
      S3BlobFs s3BlobFs) {
    this.managerConfig = managerConfig;
    this.snapshotMetadataStore = snapshotMetadataStore;
    this.meterRegistry = meterRegistry;
    this.s3BlobFs = s3BlobFs;
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Starting object storage reconciliation service");
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.info("Shutting down object storage reconciliation service");
    executorService.shutdown();
  }

  @Override
  protected void runOneIteration() throws Exception {}

  @Override
  protected Scheduler scheduler() {
    return null;
  }

  int doIt() throws IOException, ExecutionException, InterruptedException {
    /*
      We MUST first attempt to get the files before enumerating the snapshots. As these actions are not atomic, and
      can take a while to complete it is possible data is being added while we are building our sets for comparison.

      By first fetching the file paths and then the snapshots, we may incorrectly appear to have snapshots that have
      no corresponding files.
    */
    Set<String> filePaths = Sets.newHashSet(s3BlobFs.listFiles(URI.create("/"), true));
    List<SnapshotMetadata> snapshotMetadata = snapshotMetadataStore.getCached();

    Set<String> filesWithoutSnapshots = filesWithoutSnapshots(filePaths, snapshotMetadata);
    Set<SnapshotMetadata> snapshotsWithoutFiles =
        snapshotsWithoutFiles(snapshotMetadata, filePaths);

    // todo - foreach filesWithoutSnapshots, delete
    // todo - foreach snapshotsWithoutFiles, report

    return 0;
  }

  protected Set<SnapshotMetadata> snapshotsWithoutFiles(
      List<SnapshotMetadata> snapshotMetadataList, Set<String> filePaths) {
    Map<String, SnapshotMetadata> snapshotMetadataMap =
        snapshotMetadataList
            .stream()
            .collect(Collectors.toMap(SnapshotMetadata::getSnapshotPath, Function.identity()));

    Splitter splitter = Splitter.on("/");
    Set<String> filePathsExploded =
        filePaths
            .parallelStream()
            .flatMap(filePath -> explodePath(filePath, splitter).stream())
            .collect(Collectors.toCollection(HashSet::new));

    return Sets.difference(snapshotMetadataMap.keySet(), filePathsExploded)
        .stream()
        .map(snapshotMetadataMap::get)
        .collect(Collectors.toUnmodifiableSet());
  }

  protected Set<String> filesWithoutSnapshots(
      Set<String> filePaths, List<SnapshotMetadata> snapshotMetadataList) {
    Set<String> snapshotPaths =
        snapshotMetadataList
            .stream()
            .map(snapshotMetadata -> snapshotMetadata.snapshotPath)
            .collect(Collectors.toCollection(HashSet::new));

    Splitter splitter = Splitter.on("/");

    return filePaths
        .parallelStream()
        .filter(filePath -> Collections.disjoint(explodePath(filePath, splitter), snapshotPaths))
        .collect(Collectors.toUnmodifiableSet());
  }

  private List<String> explodePath(String filePath, Splitter splitter) {
    List<String> filePathParts = splitter.splitToList(filePath);
    List<String> filePathPartsSet = new ArrayList<>(filePathParts.size());
    StringBuilder stringBuilder = new StringBuilder(filePath.length());
    for (int i = 0; i < filePathParts.size(); i++) {
      stringBuilder.setLength(0);
      // add the previous string, if there is one
      if (i > 0) {
        String previousString = filePathPartsSet.get(i - 1);
        stringBuilder.append(previousString);
      }
      stringBuilder.append(filePathParts.get(i));
      // don't do the last one
      if (i < filePathParts.size() - 1) {
        stringBuilder.append("/");
      }
      filePathPartsSet.add(stringBuilder.toString());
    }
    return filePathPartsSet;
  }
}
