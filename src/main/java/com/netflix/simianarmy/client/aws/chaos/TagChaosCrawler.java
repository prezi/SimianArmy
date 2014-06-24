package com.netflix.simianarmy.client.aws.chaos;

import com.amazonaws.services.ec2.model.Instance;
import com.google.common.base.Predicate;
import com.netflix.simianarmy.GroupType;
import com.netflix.simianarmy.MonkeyConfiguration;
import com.netflix.simianarmy.basic.chaos.BasicInstanceGroup;
import com.netflix.simianarmy.chaos.ChaosCrawler;
import com.netflix.simianarmy.client.aws.AWSClient;
import org.jclouds.compute.domain.ComputeMetadata;
import org.jclouds.compute.domain.NodeMetadata;

import javax.annotation.Nullable;
import java.util.*;

/**
 * This will crawl all instances that are tagged with a certain value.
 */
public class TagChaosCrawler implements ChaosCrawler {

    public enum Types implements GroupType {
        TAG;
    }

    private final AWSClient awsClient;
    private final MonkeyConfiguration cfg;
    private static final String NS = "simianarmy.chaos.Tag.";
    private static final String TAGS_TO_CRAWL = NS.concat("tagsToCrawl");

    public static final String TYPE = "Tag";

    public TagChaosCrawler(AWSClient awsClient, MonkeyConfiguration cfg) {
        this.awsClient = awsClient;
        this.cfg = cfg;
    }

    /** {@inheritDoc} */
    @Override
    public EnumSet<?> groupTypes() {
        return EnumSet.allOf(Types.class);
    }

    /**
     * It does not make sense to call this without providing a list of tags.
     * @return
     */
    @Override
    public List<InstanceGroup> groups() {
        String tagStr = cfg.getStrOrElse(TAGS_TO_CRAWL, null);
        if (tagStr != null) {
            return groups(tagStr.split("(,|\\s)*"));
        }
        return new LinkedList<InstanceGroup>();
    }

    /** {@inheritDoc} */
    @Override
    public List<InstanceGroup> groups(String... names) {
       Map<String, List<Instance>> matchingInstances = awsClient.getInstancesMatchingAnyTags(Arrays.asList(names));
        List<InstanceGroup> list = new LinkedList<InstanceGroup>();
        for (String tag : matchingInstances.keySet()) {
            InstanceGroup ig = new BasicInstanceGroup(tag, Types.TAG, awsClient.region());
            for (Instance instance : matchingInstances.get(tag)) {
                ig.addInstance(instance.getInstanceId());
            }
            list.add(ig);
        }
        return list;
    }
}
