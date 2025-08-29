document.addEventListener("DOMContentLoaded", function () {
  const nav = document.querySelector("nav.navbar");
  if (!nav) return;

    const width = nav.clientWidth;
    const height = nav.clientHeight;
    const smooth = true;
    const nodes = [];

  // Add an extra node that will be the invisible "repeller"
    const repeller = { id: "mouse", r: 100, fx: null, fy: null };
    nodes.unshift(repeller); // insert at index 0

  const svg = d3.select(nav)
    .append("svg")
    .attr("width", width - 500)
    .attr("height", height)
    .on("pointermove", pointermoved)
    .style("position", "absolute")
    .style("top", 0)
    .style("left", 0)
    .style("pointer-events", "none") // allows clicks through the SVG
    // .attr("pointer-events", null)
    .style("z-index", 0); // stays behind nav links
    

  function pointermoved(event){
        const [x,y] = d3.pointer(event);
        repeller.fx = x;
        repeller.fy = y;
    }

  const g = svg.append("g");

  const simulation = d3.forceSimulation()
    .force("collide", d3.forceCollide().radius(17))
    .force("manyBody", d3.forceManyBody().strength(d => d.id === "mouse" ? -100 : 10))
    .force("center", d3.forceCenter(width / 2, height / 2).strength(smooth ? 0.01 : 0.01))
    .force("x", d3.forceX(width / 2).strength(0.01))
    .force("y", d3.forceY(height / 2).strength(0.01))
    .alpha(0.05)
    .alphaDecay(0);

  // Async generator for dynamic node updates
    async function startNodeUpdates() {
        while (true) {
            const [x, y] = d3.pointRadial(2 * Math.PI * Math.random(), 200);
            nodes.push({ id: Math.random(), x, y });

            // Keep only latest 20 (excluding the first which is repeller)
            if (nodes.length > 21) {
            nodes.splice(1, 1); // Remove after index 0
            }

            simulation.nodes(nodes);
            simulation.alpha(1).restart();

            await new Promise(resolve => setTimeout(resolve, 1000));
        }
    }

  simulation.on("tick", () => {
    const circles = g.selectAll("circle:not(.exit)")
      .data(nodes, d => d.id)
      .join(
        enter => enter.append("circle")
          .attr("fill", d => d.id === "mouse" ? "transparent" : d3.interpolateSinebow(d.id))
          .attr("stroke", d => d.id === "mouse" ? "none" : "black")
        //   .attr("pointer-events", "all")        // so circles don’t block clicks
          .attr("r", 1)
          .attr("transform", d => `translate(${d.x}, ${d.y})`)
          .style("pointer-events", d => d.id === "mouse" ? "none" : "auto") // <- Allow interaction for non-mouse nodes
          .transition()
          .duration(6000)
          .attr("r", 13)
          .selection(),
        update => update
          .attr("transform", d => `translate(${d.x}, ${d.y})`),
        exit => exit
          .classed("exit", true)
          .transition()
          .duration(2000)
          .attr("fill", "#eee")
          .attr("r", 2)
          .remove()
      );
  });

  startNodeUpdates();
});
