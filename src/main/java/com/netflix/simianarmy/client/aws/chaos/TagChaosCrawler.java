package com.netflix.simianarmy.client.aws.chaos;

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
    public List<InstanceGroup> groups(final String... names) {
        Set<? extends NodeMetadata> nodes = awsClient.getJcloudsComputeService().listNodesDetailsMatching(new Predicate<ComputeMetadata>() {
            @Override
            public boolean apply(@Nullable ComputeMetadata input) {
                for (String name : names) {
                    if (input.getTags().contains(name)) {
                        return true;
                    }
                }
                return false;
            }
        });
        List<InstanceGroup> list = new LinkedList<InstanceGroup>();
        for (String tag : names) {
            InstanceGroup ig = new BasicInstanceGroup(tag, Types.TAG, awsClient.region());
            for (NodeMetadata node : nodes) {
                ig.addInstance(node.getId());
            }
            list.add(ig);
        }
        return list;
    }
}
