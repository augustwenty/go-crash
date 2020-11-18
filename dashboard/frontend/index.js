// const SAMPLE_BOAT = { "Name": "Slice of Life", "Position": { "x": 200, "y": 400 }, "Velocity": { "x": 0.36431035, "y": 0.5753303 }, "Timestamp": 0.3 }
// const SAMPLE_BOAT_2 = { "Name": "Stugots", "Position": { "x": 21, "y": 40 }, "Velocity": { "x": 0.36431035, "y": -0.5753303 }, "Timestamp": 0.3 }

function onMessageReceived(messageData){
    console.log('Message from server ', messageData);
    appendToLog(messageData, logP)
    updateBoatInState(JSON.parse(messageData))
    render();
}

function onWSOpen() {
    console.log("WebSocket opened successfully")
}

function initWebSocket() {
    const socket = new WebSocket('ws://localhost:8080/?topics=boats&group.id=&auto.offset.reset=latest');
    // Connection opened
    socket.addEventListener('open', onWSOpen);

    // Listen for messages
    socket.addEventListener('message', function (event) {
        onMessageReceived(event.data)
    });
}

function appendToLog(message, log) {
    log.innerText += message + "\n";
}

function updateBoatInState(boat) {
    SimulationState[boat["Name"]] = boat
}

function render(timestamp) {
    stateP.innerText = JSON.stringify(SimulationState, null, 2)

    // clear canvas
    let canvasCtx = canvas.getContext("2d");
    canvasCtx.clearRect(0,0,1000,1000)

    // render boats
    for (const boat in SimulationState) {
        renderBoat(canvasCtx, SimulationState[boat])
    }
}

function renderBoat(canvasCtx, boat) {
    let boatName = boat["Name"]

    let boatX = boat["Position"]["x"]
    let boatY = boat["Position"]["y"]

    let boatVelX = boat["Velocity"]["x"]
    let boatVelY = boat["Velocity"]["y"]

    let boatHeading = boat["Orientation"]

    // draw boat rotated to its heading
    canvasCtx.translate(boatX, boatY);
    canvasCtx.rotate(boatHeading);
    canvasCtx.translate(-boatX, -boatY);

    // color boat based on type
    let boatType = boat["Type"]
    if (boatType == "Sailboat") {
        canvasCtx.fillStyle = "#00FF00"
    } else if (boatType == "speedboat") {
        canvasCtx.fillStyle = "#FF0000"
    } else {
        canvasCtx.fillStyle = "#9a9a9a"
    }
    canvasCtx.fillRect(boatX - 20 , boatY -5, 40, 10);

    //reset context
    canvasCtx.resetTransform()
    canvasCtx.fillStyle = "black"

    //write boat name
    canvasCtx.font = "16px Arial";
    canvasCtx.fillText(boatName, boatX - 20 , boatY - 10);
}

var SimulationState = {}
const logP = document.getElementById("log")
const stateP = document.getElementById("state")
const canvas = document.getElementById("display-canvas")

function main() {
    initWebSocket();
    render()
}

main()