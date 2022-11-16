window.onload = function() {
    drawMap('#container'); // id로 불러오기
};

import * as d3 from 'd3'

function drawMap() {
    const geojson = require('../json/dong.json');
    const center = d3.geoCentroid(geojson); // 지도 중심

    // 캔버스 설정
    const width = 1000;
    const height = 1000;

    // 지도를 그리기 위한 svg 생성
    const svg = d3
        .select('#container')
        .append('svg')
        .attr('width', width)
        .attr('height', height);
    const map = svg.append('g'); 

    let projection = d3.geoMercator() // 투사법 설정
        .scale(1)
        .translate([0, 0]); 

    const path = d3.geoPath().projection(projection); // 설정 투사법으로 그리는 지도 path

    const bounds = path.bounds(geojson); // 지도의 경계 설정

    // 축척 설정
    const widthScale = (bounds[1][0] - bounds[0][0]) / width; // 위도
    const heightScale = (bounds[1][1] - bounds[0][1]) / height; // 경도
    const scale = 1 / Math.max(widthScale, heightScale);

    // translate
    const xoffset = width/2 - scale * (bounds[1][0] + bounds[0][0])/2 + 0; 
    const yoffset = height/2 - scale * (bounds[1][1] + bounds[0][1])/2 + 0; 
    const offset = [xoffset, yoffset];
    projection.scale(scale).translate(offset);

    map
        .selectAll('path')
        .data(geojson.features)
        .enter().append('path') 
        .attr('d', path);

    // 줌 이벤트 설정
    const zoomed = () =>{
        map.attr('transform', d3.event.transform)
        icons.attr('transform', d3.event.transform)
    }
    const zoom = d3.zoom().scaleExtent([1, 8]).on('zoom', zoomed)
    svg.call(zoom)
}