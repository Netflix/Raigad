package com.netflix.raigad.dataobjects;

import com.netflix.raigad.objectmapper.DefaultMasterNodeInfoMapper;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

/*
   [
        {
            "id":"8sZZWYmmQaeNUKMq1S1uow",
            "host":"es-test-useast1d-master-i-9e112345",
            "ip":"10.111.22.333",
            "node":"us-east-1d.i-9e112345"
        }
   ]
 */
public class TestMasterNodeInfoMapper {
    ObjectMapper mapper = new DefaultMasterNodeInfoMapper();

    @Test
    public void testMasterNodeInformationObject() throws IOException {
        String masterNodeInfo = "[{\"id\":\"8sZZWYmmQaeNUKMq1S1uow\",\"host\":\"es-test-useast1d-master-i-9e112345\",\"ip\":\"10.111.22.333\",\"node\":\"us-east-1d.i-9e112345\"}]";
        try {
            List<MasterNodeInformationDO> myObjs = mapper.readValue(masterNodeInfo, new TypeReference<ArrayList<MasterNodeInformationDO>>() {
            });

            assertEquals(1,myObjs.size());

            for (MasterNodeInformationDO key : myObjs) {
                assertEquals("8sZZWYmmQaeNUKMq1S1uow",key.getId());
                assertEquals("es-test-useast1d-master-i-9e112345",key.getHost());
                assertEquals("10.111.22.333",key.getIp());
                assertEquals("us-east-1d.i-9e112345",key.getNode());
            }
        } catch (JsonGenerationException e) {
            e.printStackTrace();
        } catch (JsonMappingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

