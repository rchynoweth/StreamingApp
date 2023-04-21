// Define variables to keep track of the currently dragged box and its offset
var draggedBox = null;
var offsetX = 0;
var offsetY = 0;

// Define variables to keep track of the currently dragged line and its offsets
var draggedLine = null;
var lineOffsetX = 0;
var lineOffsetY = 0;
var lineStartX = 0;
var lineStartY = 0;
var dragging = false;


// Add and listen to a cloud drop down box. This allows us to select a workspace that we want to deploy to
function addDropdownEventListener() {
    document.addEventListener("DOMContentLoaded", function() {
    // Get a reference to the drop-down menu element
    var dropdownMenu = document.getElementById("cloud-dropdown-menu");
    // Add an event listener for the "change" event
    dropdownMenu.addEventListener("change", function () {
        // Get the selected value from the drop-down menu
        var selectedValue = dropdownMenu.value;
        fetch('/update_cloud', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({ 'cloud': selectedValue })
            });
        });
    });


}
// Call the function to attach the event listener to the drop-down menu
addDropdownEventListener();


// Function to end dragging of the box
function endDrag(event) {
    if (draggedBox) {
        // Remove the event listeners for dragging
        document.removeEventListener("mousemove", doDrag);
        document.removeEventListener("mouseup", endDrag);

        // Reset the variables for the dragged box and its offset
        draggedBox = null;
        offsetX = 0;
        offsetY = 0;
    } else if (draggedLine) {
        // Remove the event listener for dragging
        document.removeEventListener("mousemove", doLineDrag);
        document.removeEventListener("mouseup", endLineDrag);

        // Reset the variables for the dragged line and its offsets
        draggedLine = null;
        lineOffsetX = 0;
        lineOffsetY = 0;
        lineStartX = 0;
        lineStartY = 0;
        dragging = false;
    }
}

//////
// Function to add a new box to the content area
//////
function addBox(select_options) {
    // add canvas content element
    var contentDiv = document.getElementById("content");
    var newBox = document.createElement("div");
    newBox.classList.add("box");


    // Add the event listener for dragging to the new box
    newBox.addEventListener("mousedown", startDrag);

    // Add the class dropdown box to the new box
    const classSelect = createSelect(select_options, "class");
    newBox.appendChild(classSelect);

    // depending on the class selection we need to show certain options
    classSelect.addEventListener("change", function () {
        // select the class that was chosen for the box 
        var selectedOption = classSelect.options[classSelect.selectedIndex].value;

        //////
        // init variable textboxes
        //// For all the init variables in a class we need to display the text boxes for the selected class. 
        // remove old textboxes
        removeElements(newBox, "init-label-input-container");
        removeElements(newBox, "function-parameter-container");

        // get the class data then for each init variable we add a text box
        selectedClass = getSelectedObject(select_options, selectedOption);
        div = addTextBox('dataset_name')
        newBox.appendChild(div);
        for (var j = 0; j < selectedClass.init_variables.length; j++) {
            var label = selectedClass.init_variables[j];
            div = addTextBox(label)
            newBox.appendChild(div);
        }

        ////
        // function dropdown list
        // Show the functions that are available for the class that was selected 
        ////
        removeElements(newBox, "function-select");

        functionSelect = document.createElement("select");
        functionSelect.name = "functionSelect";
        functionSelect.id = "functionSelect";
        functionSelect.classList.add("function-select");

        for (var j = 0; j < selectedClass.function_options.length; j++) {
            var option = document.createElement("option");
            option.value = selectedClass.function_options[j];
            option.text = selectedClass.function_options[j];
            functionSelect.appendChild(option);
        }

        ///// 
        // inner event listener of function select 
        // We need to listen if the function selection changes and we need to display the appropriate function parameters as text boxes
        functionSelect.addEventListener("change", function () {
            // select the class that was chosen for the box 

            // Get the function we selected above
            var selectedFunctionValue = functionSelect.options[functionSelect.selectedIndex].value;

            // remove old textboxes
            removeElements(newBox, "function-parameter-container");

            // for each parameter for the Class.function we need to display the options. 
            var selectedParameters = getSelectedObject(selectedClass.function_parameters, selectedFunctionValue);
            for (var k = 0; k < selectedParameters.function_parameters.length; k++) {
                var label = selectedParameters.function_parameters[k];
                var div = document.createElement("div");
                div.classList.add("function-parameter-container");

                var labelElem = document.createElement("label");
                labelElem.innerText = label;
                labelElem.classList.add("input-label");
                div.appendChild(labelElem);

                var inputElem = document.createElement("input");
                inputElem.type = "text";
                inputElem.classList.add("input-textbox");
                div.appendChild(inputElem);

                newBox.appendChild(div);

            }
        });// inner event


        newBox.appendChild(functionSelect);
    }); // parent event listener


    contentDiv.appendChild(newBox);
} // function

// Create a text box for the init variables 
function addTextBox(label) {
    var div = document.createElement("div");
    div.classList.add("init-label-input-container");

    var labelElem = document.createElement("label");
    labelElem.innerText = label;
    labelElem.classList.add("input-label");
    div.appendChild(labelElem);

    var inputElem = document.createElement("input");
    inputElem.type = "text";
    inputElem.classList.add("input-textbox");
    div.appendChild(inputElem);
    return div;
}

// Function to execute when the execute_pipeline button is clicked 
function createPipeline() {

    var data = getPipelineData()
    console.log(data)


    fetch('/create_pipeline', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json'
        },
        body: JSON.stringify(data)
    });
}

// function to execute when user clicks the update pipeline button
function updatePipeline() {
    var data = getPipelineData()
    console.log(data)


    fetch('/update_pipeline', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json'
        },
        body: JSON.stringify(data)
    });
}

// function to execute when user clicks the delete pipeline button
function deletePipeline() {
    var data = getPipelineData()
    console.log(data)


    fetch('/start_pipeline', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json'
        },
        body: JSON.stringify(data)
    });
}

// function to execute when user clicks the start pipeline button
function startPipeline() {
    var data = getPipelineData()
    console.log(data)


    fetch('/start_pipeline', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json'
        },
        body: JSON.stringify(data)
    });
}

// Whenever a button is clicked we need to gather all the data from the form fields. 
function getPipelineData() {
    // gathers all the inputted information from the boxes and left panel
    var boxes = document.querySelectorAll(".box");
    var data = [];

    // For each box collect the init parameters and function parameters and add it to the list
    boxes.forEach(function (box) {
        var boxData = {
            class: box.querySelector(".class").value,
            init_variables: [],
            function: "",
            function_parameters: []
        };

        var initLabelInputs = box.querySelectorAll(".init-label-input-container input");
        initLabelInputs.forEach(function (input) {
            boxData.init_variables.push(input.value);
        });

        var functionSelect = box.querySelector(".function-select");
        if (functionSelect) {
            boxData.function = functionSelect.value;

            var functionParamInputs = box.querySelectorAll(".function-parameter-container input");
            functionParamInputs.forEach(function (input) {
                boxData.function_parameters.push(input.value);
            });
        }

        data.push(boxData);
    });

    // collect the text box information on the left panel. 
    var textBoxes = document.querySelectorAll('.left-panel input[type="text"]');
    data.push(textBoxes)

    // format the json data for the backend 
    json_data = {
        'job_name': data[data.length - 1][0].value,
        'target_database': data[data.length - 1][1].value,
        'working_directory': data[data.length - 1][2].value,
        'boxes': []
    }

    for (var i = 0; i < data.length - 1; i++) {
        json_data.boxes.push(data[i])
    }

    return json_data;
}

// allows us to parse the data easier 
function getSelectedObject(selectedOptions, selectedOption) {
    for (var i = 0; i < selectedOptions.length; i++) {
        if (selectedOptions[i].name == selectedOption) {
            return selectedOptions[i]
        }
    }
}

// Helps us select which class we chose for a box
function createSelect(options, className) {
    const select = document.createElement("select");
    select.name = className;
    select.classList.add(className);

    for (const option of options) {
        const { name } = option;
        const optionElem = document.createElement("option");
        optionElem.value = name;
        optionElem.text = name;
        select.appendChild(optionElem);
    }

    return select;
}

// removes the old text boxes when a user changes the class/function 
function removeElements(parent, className) {
    const oldElements = parent.querySelectorAll(`.${className}`);
    for (const elem of oldElements) {
        parent.removeChild(elem);
    }
}

// Function to start dragging a box
function startDrag(event) {
    // Save the box/line being dragged and its offset/offsets
    if (event.target.classList.contains("box")) {
        draggedBox = event.target;
        offsetX = event.clientX - draggedBox.offsetLeft;
        offsetY = event.clientY - draggedBox.offsetTop;

        // Set the box's position to absolute and its z-index to 1 to bring it to the front
        draggedBox.style.position = "absolute";
        draggedBox.style.zIndex = "1";
    } else if (event.target.classList.contains("line")) {
        draggedLine = event.target;
        lineOffsetX = event.clientX - draggedLine.offsetLeft;
        lineOffsetY = event.clientY - draggedLine.offsetTop;
        lineStartX = draggedLine.offsetLeft;
        lineStartY = draggedLine.offsetTop;

        // Set dragging to true to indicate that the line is being dragged
        dragging = true;
    }

    // Add event listeners to handle dragging
    document.addEventListener("mousemove", doDrag);
    document.addEventListener("mouseup", endDrag);
}

// Function to perform dragging of the box/line
function doDrag(event) {
    if (draggedBox) {
        // Calculate the new position of the box based on the mouse position and the offset
        var newX = event.clientX - offsetX;
        var newY = event.clientY - offsetY;

        // Set the position of the box to the new position
        draggedBox.style.left = newX + "px";
        draggedBox.style.top = newY + "px";
    } else if (draggedLine) {
        // Calculate the new position of the line and its endpoints based on the mouse position and the offsets
        var newX = event.clientX - lineOffsetX;
        var newY = event.clientY - lineOffsetY;

        // Move the line to the new position
        draggedLine.style.left = newX + "px";
        draggedLine.style.top = newY + "px";

        // Calculate the new positions of the line endpoints based on the position of the line
        var startX = newX + lineStartX;
        var startY = newY + lineStartY;
        var endX = startX + draggedLine.offsetWidth;
        var endY = startY;

        // Move the endpoints to their new positions
        startPoint.style.left = startX + "px";
        startPoint.style.top = startY + "px";
        endPoint.style.left = endX + "px";
        endPoint.style.top = endY + "px";
    }
}


// Function to end dragging of the box/line
function endDrag(event) {
    // Remove the event listeners for dragging
    document.removeEventListener("mousemove", doDrag);
    document.removeEventListener("mouseup", endDrag);

    // Reset the variables for the dragged box and its offset
    draggedBox = null;
    offsetX = 0;
    offsetY = 0;
}
function addLine() {
    var contentDiv = document.getElementById("content");
    var newLine = document.createElement("div");
    newLine.classList.add("line");
    newLine.style.width = "100px";
    newLine.style.height = "5px";
    newLine.style.backgroundColor = "black";

    newLine.addEventListener("mousedown", startDrag)
    newLine.addEventListener("mouseup", endDrag)
    contentDiv.appendChild(newLine);

}