// Handle Enter Key press in sector weight inputs
window.addEventListener('DOMContentLoaded', function() {
    // Add a global keydown event listener for Enter key press in weight inputs
    document.addEventListener('keydown', function(event) {
        // Only respond to Enter key
        if (event.key === 'Enter') {
            // Check if the active element is a weight input field
            var activeElement = document.activeElement;
            
            if (activeElement && 
                activeElement.tagName === 'INPUT' && 
                activeElement.id && 
                activeElement.id.includes('weight-input')) {
                
                event.preventDefault();
                
                // Extract the sector from the input ID
                // Format is {"type":"weight-input","index":"SectorName"}
                try {
                    var idObject = JSON.parse(activeElement.id);
                    if (idObject && idObject.type === 'weight-input' && idObject.index) {
                        var sector = idObject.index;
                        
                        // Find the corresponding Apply button and click it
                        var applyButtonId = '{"type":"apply-weight","index":"' + sector + '"}';
                        var applyButton = document.querySelector('button[id="' + applyButtonId.replace(/"/g, '\\"') + '"]');
                        
                        if (applyButton) {
                            applyButton.click();
                        } else {
                            // Alternative approach: find buttons that include the sector name
                            var buttons = document.querySelectorAll('button[id*="apply-weight"][id*="' + sector + '"]');
                            if (buttons.length > 0) {
                                buttons[0].click();
                            }
                        }
                    }
                } catch (e) {
                    console.log("Error parsing input ID: ", e);
                }
            }
        }
    });
});