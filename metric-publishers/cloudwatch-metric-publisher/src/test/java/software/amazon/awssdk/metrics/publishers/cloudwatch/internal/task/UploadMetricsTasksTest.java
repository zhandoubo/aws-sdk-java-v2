package software.amazon.awssdk.metrics.publishers.cloudwatch.internal.task;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import software.amazon.awssdk.metrics.publishers.cloudwatch.internal.MetricUploader;
import software.amazon.awssdk.metrics.publishers.cloudwatch.internal.transform.MetricCollectionAggregator;
import software.amazon.awssdk.services.cloudwatch.model.PutMetricDataRequest;

public class UploadMetricsTasksTest {
    private MetricCollectionAggregator aggregator;
    private MetricUploader uploader;
    private UploadMetricsTasks task;

    @Before
    public void setUp() {
        aggregator = Mockito.mock(MetricCollectionAggregator.class);
        uploader = Mockito.mock(MetricUploader.class);
        task = new UploadMetricsTasks(aggregator, uploader, 2);
    }


    @Test
    public void extraTasksAboveMaximumAreDropped() {
        List<PutMetricDataRequest> requests = Arrays.asList(PutMetricDataRequest.builder().build(),
                                                            PutMetricDataRequest.builder().build(),
                                                            PutMetricDataRequest.builder().build());
        Mockito.when(aggregator.getRequests()).thenReturn(requests);
        task.run();


        ArgumentCaptor<List> captor = ArgumentCaptor.forClass(List.class);
        Mockito.verify(uploader).upload(captor.capture());
        List<PutMetricDataRequest> uploadedRequests = captor.getValue();

        assertThat(uploadedRequests).hasSize(2);
        assertThat(uploadedRequests).containsOnlyElementsOf(requests);
    }
}