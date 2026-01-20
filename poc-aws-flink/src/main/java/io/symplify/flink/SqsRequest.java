package io.symplify.flink;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SqsRequest {
    private String queueUrl;
    private String body;
    private String deduplicationId;
    private String messageGroupId;
}
