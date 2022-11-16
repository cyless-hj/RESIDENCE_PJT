window.onload = function() {
    drawMap('#container'); // id로 불러오기
};

//지도 그리기
function drawMap(target) {
    // 캔버스 설정
    var width = 800;
    var height = 200;
    
    var initialScale = 200000;
    var initialX = -11550; //초기 위치값 X
    var initialY = 4100; //초기 위치값 Y
    var centered;
    var labels;

    // 투사법 설정
    var projection = d3.geo.mercator()
        .scale(initialScale)
        .center([126.9895, 37.5651])
        .translate([width/2, height/2]);


    // 지도 path
    var path = d3.geo.path()
        .projection(projection);

    //줌
    var zoom = d3.behavior.zoom()
        .translate(projection.translate())
        .scale(projection.scale())
        .scaleExtent([height, 800 * height])
        .on('zoom', zoom);

    // 지도를 그릴 svg 설정
    var svg = d3.select(target).append('svg')
        .attr('width', width)
        .attr('height', height)
        .attr('id', 'map')
        .attr('class', 'map');

    var states = svg.append('g')
        .attr('id', 'states')
        .call(zoom);
        
    // var place = svg.append(g)
    //     .attr("id","place");

    states.append('rect')
        .attr('class', 'background')
        .attr('width', width)
        .attr('height', height);

    //geoJson데이터를 파싱하여 지도그리기
    d3.json('../static/json/dong.json', function(json) {
        states.selectAll('path') //지역 설정
            .data(json.features)
            .enter().append('path')
            .attr('d', path)
            .attr('id', function(d) {
                return 'path-' + d.properties.sidonm;
            })
            .on("click",click);

        labels = states.selectAll('text')
            .data(json.features) //라벨표시
            .enter().append('text')
            .attr('transform', translateTolabel)
            .attr('id', function(d) {
                return 'label-' + d.properties.sidonm;
            })
            .attr('text-anchor', 'middle')
            .attr('dy', '.35em')
            .on("click", click)
            .text(function(d) {
                return d.properties.adm_nm; // 동 이름 표시
            });
    });

    function click(d) {
        var x, y, k;

        if (d && centered !== d) {
            var centroid = path.centroid(d);
            x = centroid[0];
            y = centroid[1];
            k = 4;
            centered = d;
            } else {
            x = width / 2;
            y = height / 2;
            k = 1;
            centered = null;
        }
        states.selectAll("path")
            .classed("active", centered && function(d) { return d === centered; });
    
        states.transition()
            .duration(500)
            .attr("transform", "translate(" + width / 2 + "," + height / 2 + ")scale(" + k + ")translate(" + -x + "," + -y + ")")
            .style("stroke-width", 1.5 / k + "px");
        var element = document.getElementById("dong_code");
        element.value = d.properties.adm_cd2;
        console.log(element);
    }

    //텍스트 위치 조절 - 하드코딩으로 위치 조절을 했습니다.
    function translateTolabel(d) {
        var arr = path.centroid(d);
        return 'translate(' + arr + ')';
    }

    function zoom() {
        projection.translate(d3.event.translate).scale(d3.event.scale);
        states.selectAll('path').attr('d', path);
        labels.attr('transform', translateTolabel);
    }
}
