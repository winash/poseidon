package poseidon.tests;

import com.tinkerpop.gremlin.Gremlin;


import org.junit.Test
import com.tinkerpop.blueprints.pgm.util.graphml.GraphMLReader;

/**
 * @author Ashwin
 */
public class HGraphGremlinIntegrationTest extends HBaseTestBase {

  @Override
  public void setUp() {
    Gremlin.load();
    super.setUp();
  }

  @Test
  public void shouldCountVerticesAndEdgesWithGremlin() {
    final GraphMLReader graphMLReader = new GraphMLReader(graph);
    graphMLReader.inputGraph(GraphMLReader.class.getResourceAsStream("graph-example-3.xml"));
    def nodes = graph.V
    def nodesList = [];
    nodes.each{
      nodesList << it
    }
    def edgesList = [];
    def edges = graph.E
     edges.each{
      edgesList << it
    }

    assert nodesList.size() == 6
    assert edgesList.size() == 6

  }


  @Test
  public void shouldTraverseSimplePaths(){
    final GraphMLReader graphMLReader = new GraphMLReader(graph);
    graphMLReader.inputGraph(GraphMLReader.class.getResourceAsStream("graph-example-1.xml"));
    def nodes = graph.V
    def marko = nodes.find {it.name == "marko"}
    def v = graph.v(marko.id)
    assert v.name == "marko"
    assert v.age == 29
    def outE = v.outE('knows').inV
    def markoKnows = outE.collect { it.name}
    assert markoKnows.size() == 2
    assert markoKnows.contains("vadas")
    assert markoKnows.contains("josh")
    outE = v.outE('created').inV
    def markoCreated = outE.collect { it.name}
    assert markoCreated.size() == 1
    assert markoCreated.contains("lop")

  }


  @Test
  public void shouldTraverseComplexPaths(){
    final GraphMLReader graphMLReader = new GraphMLReader(graph);
    graphMLReader.inputGraph(GraphMLReader.class.getResourceAsStream("graph-example-1.xml"));
    def nodes = graph.V
    def marko = nodes.find {it.name == "marko"}
    def v = graph.v(marko.id)
    def name =  v.outE{it.label=='knows'}.inV{it.age > 30}.name
    name.each{
      assert it == "josh"
    }

  }






}
