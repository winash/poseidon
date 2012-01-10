package poseidon.integrationtests;

import com.tinkerpop.blueprints.pgm.Vertex;
import poseidon.HGraph;
import org.junit.Test;

import java.util.ArrayList;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
/**
 * Please make sure you have a hadoop and hbase running correctly
 *
 * @author Ashwin
 */
public class HGraphIntegrationTest {

   @Test                         
   public void testInsertVertices(){
       HGraph hGraph = new HGraph("localhost", "2184", "lookowt", true);
       
       for(int i = 0; i < 100000;i++){
            System.out.println(i);
            hGraph.addVertex(null);
       }
       Iterable<Vertex> iterable = hGraph.getVertices();
       ArrayList<Vertex> vertexArrayList = (ArrayList<Vertex>) iterable;
       assertThat(vertexArrayList.size(),is(100000));
       for(Vertex vertex:vertexArrayList){
           hGraph.removeVertex(vertex);
       }
       iterable = hGraph.getVertices();
       vertexArrayList = (ArrayList<Vertex>) iterable;
       assertThat(vertexArrayList.size(),is(0));


   }

    


}
