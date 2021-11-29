package adapters;

import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;

import java.util.ArrayList;
import java.util.List;

public class EC2Adapter {

    private Ec2Client ec2 = Ec2Client.create();

    public EC2Adapter() {
        ec2 = Ec2Client.builder().credentialsProvider(DefaultCredentialsProvider.create()).build();
    }

    public String createEC2Instance(String name, String userData) {


        String amiId = "ami-04ad2567c9e3d7893";
        List<String> securityGroupIds = new ArrayList();
        securityGroupIds.add("sg-067f67df6351d14ad");

        RunInstancesRequest runRequest = RunInstancesRequest.builder()
                .imageId(amiId)
                .instanceType(InstanceType.T2_MICRO)
                .maxCount(1)
                .minCount(1)
                .userData(userData)
                .keyName("program-key")
                .securityGroupIds(securityGroupIds)
                .build();

        RunInstancesResponse response = ec2.runInstances(runRequest);
        String instanceId = response.instances().get(0).instanceId();

        Tag tag = Tag.builder()
                .key("Name")
                .value(name)
                .build();

        CreateTagsRequest tagRequest = CreateTagsRequest.builder()
                .resources(instanceId)
                .tags(tag)
                .build();

        ec2.createTags(tagRequest);

        return instanceId;

    }

    public StartInstancesResponse startInstance(String instanceId) {

        StartInstancesRequest request = StartInstancesRequest.builder()
                .instanceIds(instanceId)
                .build();

        return ec2.startInstances(request);
    }

    public StopInstancesResponse stopInstance(String instanceId) {

        StopInstancesRequest request = StopInstancesRequest.builder()
                .instanceIds(instanceId)
                .build();

        return ec2.stopInstances(request);
    }

    public RebootInstancesResponse rebootEC2Instance(String instanceId) {
        RebootInstancesRequest request = RebootInstancesRequest.builder()
                .instanceIds(instanceId)
                .build();
        return ec2.rebootInstances(request);
    }

    public TerminateInstancesResponse terminateEC2Instance(String instanceId) {
        TerminateInstancesRequest request = TerminateInstancesRequest.builder()
                .instanceIds(instanceId)
                .build();
        return ec2.terminateInstances(request);
    }

    public List<Instance> describeEC2Instances() {
        List<Instance> instances = new ArrayList<>();
        DescribeInstancesResponse response = null;
        Ec2Client ec2 = Ec2Client.create();
        try {
            DescribeInstancesRequest request = DescribeInstancesRequest.builder().maxResults(6).nextToken(null).build();
            response = ec2.describeInstances(request);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }


        for (Reservation res : response.reservations()) {
            instances.addAll(res.instances());
        }
        return instances;

    }
}
