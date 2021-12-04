package adapters;

import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;

import java.util.ArrayList;
import java.util.List;

import static java.lang.Thread.sleep;

public class EC2Adapter {

    private Ec2Client ec2;

    public EC2Adapter() {
        ec2 = Ec2Client.builder().credentialsProvider(DefaultCredentialsProvider.create()).build();
    }

    public String createEC2Instance(String name, String userData, InstanceType instanceType) {


        String amiId = "ami-04ad2567c9e3d7893";
        List<String> securityGroupIds = new ArrayList<>();
        securityGroupIds.add("sg-067f67df6351d14ad");

        RunInstancesRequest runRequest = RunInstancesRequest.builder()
                .imageId(amiId)
                .instanceType(instanceType)
                .maxCount(1)
                .minCount(1)
                .userData(userData)
                .keyName("program-key")
                .securityGroupIds(securityGroupIds)
                .build();

        RunInstancesResponse response = ec2.runInstances(runRequest);
        String instanceId = response.instances().get(0).instanceId();
        try {
            sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
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

    public void terminateEC2Instance(String instanceId) {
        TerminateInstancesRequest request = TerminateInstancesRequest.builder()
                .instanceIds(instanceId)
                .build();
        ec2.terminateInstances(request);
    }

    public List<Instance> describeEC2Instances() {
        List<Instance> instances = new ArrayList<>();
        DescribeInstancesResponse response = null;
        Ec2Client ec2 = Ec2Client.create();
        try {
            DescribeInstancesRequest request = DescribeInstancesRequest.builder().build();
            response = ec2.describeInstances(request);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }


        assert response != null;
        for (Reservation res : response.reservations()) {
            instances.addAll(res.instances());
        }
        return instances;

    }
}
