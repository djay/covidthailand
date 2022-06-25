import html
import json
import xml.etree.ElementTree as ET
from io import BytesIO

import matplotlib


def save_svg_with_crosshairs(plt, path, legend, x, series_table, series=None, **savefig_params):
    """
    Save the plt to path as an svg including javascript that shows an x crosshair giving
    detail about each value at that x value. You can optionally display multiple values
    for each series even if not included in the plot data. series_table is a dict of
    "Column Name" -> values list.
    """

    # TODO: can we get x and series from the plt?
    # if not display_dataframes:
    #     display_dataframes = {"": position_dataframe}

    # TODO: get legend from plt. but how?

    # Save as SVG so we can extend it with our crosshairs
    f = BytesIO()
    plt.savefig(f, format="svg", **savefig_params)

    # Create XML tree from the SVG file.
    tree, xmlid = ET.XMLID(f.getvalue())
    tree.set('onload', 'init(event)')

    # Get colours and labels for series displayed on the legend
    colours = []
    legends = []
    for number, patch in enumerate(legend.get_patches() or legend.get_lines()):
        text = legend.get_texts()[number].get_text()
        text = html.escape(text).encode('ascii', 'xmlcharrefreplace').decode("utf8")
        color = list(patch.get_facecolor() if hasattr(patch, "get_facecolor") else patch.get_color())
        legends.append(text)
        colour = matplotlib.colors.to_hex(color, keep_alpha=False)
        colours.append(colour)

    # Insert SVG for the line, circles and the tooltip itself
    circles = [f'<circle id="dot_{number}" r="5" fill="{colour}" />' for number, colour in enumerate(colours)]
    tooltipsvg = f"""
    <g  xmlns="http://www.w3.org/2000/svg" pointer-events="none" class="tooltip mouse" visibility="hidden" style="background:#0000ff50;">
            <foreignObject id="tooltiptext" width="700" height="750" style="overflow:visible">
            <body xmlns="http://www.w3.org/1999/xhtml" >
            <div style="border:1px solid white; padding: 10px; color: white;  display:table; background-color: rgb(0, 0, 0, 0.60); font-family: 'DejaVu Sans', sans-serif;">
                <table id="tooltip_table">
                </table>
            </div>
            </body>
            </foreignObject>
    </g>
    """
    linesvg = f"""
    <g id="date_line" xmlns="http://www.w3.org/2000/svg" pointer-events="none" visibility="hidden">
        <line x1="500" y1="0" x2="500" y2="2000"  style="fill:none;stroke:#808080;stroke-dasharray:3.7,1.6;stroke-dashoffset:0;"/>
        {"".join(circles)}
    </g>
    """
    xmlid["figure_1"].append(ET.XML(tooltipsvg))
    xmlid["figure_1"].append(ET.XML(linesvg))
    xmlid["figure_1"].set("fill", "black")  # some browsers don't seem to respect background

    # Add the data into the script. handles either dataframes or lists
    data = [d.to_json(orient="columns", date_format="iso") if hasattr(d, "to_json") else json.dumps(list(d))
            for d in series_table.values()]
    datajs = f"""
        var x_index = {json.dumps(x)};
        var data = [{",".join(data)}];
        var series = {"null" if not series else json.dumps(series)};
        var colours = {json.dumps(colours)};
        var legends = {json.dumps(legends)};
        var headings = {json.dumps(list(series_table.keys()))};
        """

    # Insert the script at the top of the file and save it.
    tree.insert(
        0, ET.XML(f'<script type="text/ecmascript" xmlns="http://www.w3.org/2000/svg"><![CDATA[{SCRIPT};{datajs}]]></script>'))
    tree.insert(0, ET.XML('<script href="https://d3js.org/d3.v7.min.js" xmlns="http://www.w3.org/2000/svg"></script>'))

    ET.ElementTree(tree).write(path)


# This is the script for adding mousemove and mouseout to display the line, circles and tooltip at the right place
SCRIPT = """
    function init(event) {
        var tooltip = d3.select("g.tooltip.mouse");
        var line = d3.select("g#date_line line");
        var plot = d3.select("#patch_2");
        var offset = plot.node().getBBox().x;
        var date_label = d3.select("#date");
        // var border = d3.select("#tooltiprect");
        var gap = 15;
        let padding = 4;
        if (!series) {
            series = data[0];
        }

        d3.select("#figure_1").on("mousemove", function (evt) {
            // from https://codepen.io/billdwhite/pen/rgEbc
            tooltip.attr('visibility', "visible")
            var plotpos = d3.pointer(evt, plot.node())[0] - offset;
            var index = Math.round(plotpos / plot.node().getBBox().width * (x_index.length-1));
            var date = x_index[index];
            if (!date) {
                tooltip.attr('visibility', "hidden");
                d3.select("g#date_line").attr('visibility', "hidden");
                d3.select("#legend_1").attr('visibility', "visible");
                return;
            }
            else if ((typeof date === 'string' || date instanceof String) && date.includes("T")) {
                // HACK: strip off timezone
                date = date.split("T")[0];
            }
            //date_label.node().textContent = date;
            values = [];
            for ( let number = 0; number < legends.length; number++ ) {
                var row = [series[index][number], legends[number], colours[number]];
                for (let d = 0; d < data.length; d++) {
                    row.push(data[d][index][number])
                }
                values.push(row);
            }
            values.sort(function(a,b) {return a[0] - b[0]});
            values.reverse();

            table = "<html:tr><html:th>"+(new Date(date)).toDateString()+"</html:th>";
            for (let l = 0; l < headings.length; l++) {
                table += "<html:th>"+headings[l]+"</html:th>";
            }
            table += "</html:tr>";
            for (let col = 0; col < values.length; col++) {
                var colour = values[col][2];
                table += "<html:tr><html:td style='color:" + colour + "'>" + values[col][1] + "</html:td>";
                for ( let number = 3; number < values[col].length; number++ ) {
                    table += "<html:td style='text-align: right'>" + values[col][number] + "</html:td>";
                }
                table += "</html:tr>";
            }
            d3.select("#tooltip_table").html(table);

            var mouseCoords = d3.pointer(evt, tooltip.node().parentElement);
            let tooltipbox = d3.select("#tooltiptext div").node();
            let width = tooltipbox.clientWidth;
            var x = mouseCoords[0] - width - gap*2;
            if (x < 0) {
                x = mouseCoords[0] + gap;
            }
            tooltip
                .attr("transform", "translate("
                    + (x) + ","
                    + (mouseCoords[1] - tooltipbox.clientHeight/2) + ")");
            line.attr("x1", mouseCoords[0]);
            line.attr("x2", mouseCoords[0]);
            let top = plot.node().getBBox().y;
            let bottom = top + plot.node().getBBox().height;
            line.attr("y1", top);
            line.attr("y2", bottom);
            d3.select("#date_line").attr('visibility', "visible");
            d3.select("#legend_1").attr('visibility', "hidden");

            // Move the dots
            for (let col = 0; col < legends.length; col++) {
                let dot = d3.select("#dot_"+col);
                dot.attr('cy', bottom - (series[index][col] * (bottom - top)) );
                dot.attr('cx', mouseCoords[0]);
                if (series[index][col] == null) {
                    dot.attr("visibility", "hidden");
                }
                else {
                    dot.attr("visibility", "visible");
                }
            }


        })
        .on("mouseout", function () {
            d3.select("#date_line").attr('visibility', "hidden");
            return tooltip.attr('visibility', "hidden");
            d3.select("#legend_1").attr('visibility', "visible");
        });

    }
    """

if __name__ == "__main__":
    # %%
    import matplotlib.pyplot as plt

    # data from United Nations World Population Prospects (Revision 2019)
    # https://population.un.org/wpp/, license: CC BY 3.0 IGO
    year = [1950, 1960, 1970, 1980, 1990, 2000, 2010, 2018]
    population_by_continent = {
        'africa': [228, 284, 365, 477, 631, 814, 1044, 1275],
        'americas': [340, 425, 519, 619, 727, 840, 943, 1006],
        'asia': [1394, 1686, 2120, 2625, 3202, 3714, 4169, 4560],
        'europe': [220, 253, 276, 295, 310, 303, 294, 293],
        'oceania': [12, 15, 19, 22, 26, 31, 36, 39],
    }

    fig, ax = plt.subplots()
    ax.stackplot(year, population_by_continent.values(),
                 labels=population_by_continent.keys(), alpha=0.8)
    legend = ax.legend(loc='upper left')
    ax.set_title('World population')
    ax.set_xlabel('Year')
    ax.set_ylabel('Number of people (millions)')
    # %%
    save_svg_with_crosshairs(plt, "crosshairs.svg", legend, year, {"Population": population_by_continent.values()})
    # plt.show()
