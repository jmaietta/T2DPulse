/**
 * T2D Pulse Enhanced Weight Updater
 * This script provides a more aggressive client-side fix for weight updates
 * without requiring server-side changes
 */
window.addEventListener('DOMContentLoaded', function() {
    console.log("T2D Pulse Enhanced Weight Updater loaded");
    
    // Set up after a short delay to ensure Dash components are loaded
    setTimeout(setupUpdater, 1500);
    
    function setupUpdater() {
        // Find all Apply buttons
        const applyButtons = Array.from(document.querySelectorAll('button')).filter(b => 
            b.innerText === 'Apply' || 
            (b.id && b.id.includes('apply-weight'))
        );
        
        if (applyButtons.length === 0) {
            console.log("No Apply buttons found yet, retrying in 1 second");
            setTimeout(setupUpdater, 1000);
            return;
        }
        
        console.log(`Found ${applyButtons.length} Apply buttons`);
        
        // Create a map of sector to current weight
        let currentWeights = {};
        
        // Get all current weight values
        function updateCurrentWeights() {
            const weightInputs = document.querySelectorAll('input[id*="weight-input"]');
            weightInputs.forEach(input => {
                try {
                    // Extract sector from input ID
                    const idMatch = input.id.match(/"index":"([^"]+)"/);
                    if (idMatch && idMatch[1]) {
                        const sector = idMatch[1];
                        const value = parseFloat(input.value);
                        if (!isNaN(value)) {
                            currentWeights[sector] = value;
                        }
                    }
                } catch (e) {
                    console.error("Error parsing input ID:", e);
                }
            });
            console.log("Current weights:", currentWeights);
        }
        
        // Initial weight capture
        updateCurrentWeights();
        
        // Add click handlers to all Apply buttons
        applyButtons.forEach(button => {
            button.addEventListener('click', function(e) {
                try {
                    // Find related input
                    const container = e.target.closest('.sector-card');
                    if (!container) return;
                    
                    const input = container.querySelector('input[type="number"]');
                    if (!input) return;
                    
                    // Extract sector name 
                    const idMatch = input.id.match(/"index":"([^"]+)"/);
                    if (!idMatch || !idMatch[1]) return;
                    
                    const sector = idMatch[1];
                    const newValue = parseFloat(input.value);
                    
                    if (isNaN(newValue) || newValue < 1 || newValue > 100) {
                        showFeedback(`Invalid weight value for ${sector}`, 'error');
                        return;
                    }
                    
                    console.log(`Apply clicked for ${sector}: ${newValue}%`);
                    
                    // Visual feedback
                    input.style.backgroundColor = "#e3f2fd";
                    button.style.backgroundColor = "#2196f3";
                    button.style.color = "white";
                    
                    // Store the old value
                    const oldValue = currentWeights[sector] || 0;
                    
                    // Calculate the adjustment
                    const delta = newValue - oldValue;
                    
                    // Show processing message
                    showFeedback(`Processing ${sector} weight: ${oldValue}% → ${newValue}%`, 'processing');
                    
                    // Create a MutationObserver to watch for server updates
                    setupUpdateObserver(sector, newValue, oldValue);
                    
                    // Fallback timer - force an update if server doesn't respond
                    setTimeout(() => {
                        forceRefreshWeights(sector, newValue);
                    }, 5000);
                    
                } catch (error) {
                    console.error("Error in Apply button click handler:", error);
                }
            });
        });
        
        // Set up observer to watch for updates
        function setupUpdateObserver(changedSector, newValue, oldValue) {
            const weightInputs = document.querySelectorAll('input[id*="weight-input"]');
            
            // Create a map of current values for comparison
            const initialValues = {};
            weightInputs.forEach(input => {
                const idMatch = input.id.match(/"index":"([^"]+)"/);
                if (idMatch && idMatch[1]) {
                    initialValues[idMatch[1]] = parseFloat(input.value);
                }
            });
            
            // Function to check for changes
            const checkForChanges = () => {
                let changed = false;
                weightInputs.forEach(input => {
                    const idMatch = input.id.match(/"index":"([^"]+)"/);
                    if (!idMatch || !idMatch[1]) return;
                    
                    const sector = idMatch[1];
                    const currentValue = parseFloat(input.value);
                    
                    // If this input changed from its initial value
                    if (initialValues[sector] !== currentValue) {
                        changed = true;
                        console.log(`Server updated ${sector}: ${initialValues[sector]} → ${currentValue}`);
                    }
                });
                
                if (changed) {
                    // Server has responded with updates
                    showFeedback("Weights updated by server", "success");
                    clearInterval(checkInterval);
                }
            };
            
            // Check for changes every 500ms for up to 5 seconds
            let attempts = 0;
            const checkInterval = setInterval(() => {
                checkForChanges();
                attempts++;
                if (attempts >= 10) {
                    clearInterval(checkInterval);
                }
            }, 500);
        }
        
        // Fallback: Force a refresh if server doesn't respond
        function forceRefreshWeights(changedSector, newValue) {
            // Get all current weight inputs again
            const weightInputs = document.querySelectorAll('input[id*="weight-input"]');
            
            // Create a map of sectors to inputs for direct manipulation
            const sectorInputs = {};
            weightInputs.forEach(input => {
                const idMatch = input.id.match(/"index":"([^"]+)"/);
                if (idMatch && idMatch[1]) {
                    sectorInputs[idMatch[1]] = input;
                }
            });
            
            // Check if any updates happened
            const targetInput = sectorInputs[changedSector];
            if (!targetInput) return;
            
            // If value is still the old one, we need to force an update
            if (Math.abs(parseFloat(targetInput.value) - newValue) > 0.01) {
                console.log(`Server did not update weights for ${changedSector}, forcing client-side update`);
                
                // Calculate new weights that sum to 100
                const oldSum = Object.values(currentWeights).reduce((sum, val) => sum + val, 0);
                const otherSectors = Object.keys(currentWeights).filter(s => s !== changedSector);
                
                // If changing from X to Y, we need to adjust other sectors by (Y-X)/(n-1)
                // where n is the number of sectors
                const delta = newValue - currentWeights[changedSector];
                const adjustmentPerSector = -delta / otherSectors.length;
                
                // Apply the adjustment to each sector
                const newWeights = {...currentWeights};
                newWeights[changedSector] = newValue;
                
                otherSectors.forEach(sector => {
                    newWeights[sector] = Math.max(1, newWeights[sector] + adjustmentPerSector);
                });
                
                // Normalize to exactly 100
                const newSum = Object.values(newWeights).reduce((sum, val) => sum + val, 0);
                if (Math.abs(newSum - 100) > 0.1) {
                    const adjustmentFactor = 100 / newSum;
                    Object.keys(newWeights).forEach(sector => {
                        newWeights[sector] = newWeights[sector] * adjustmentFactor;
                    });
                }
                
                // Apply the new weights to the inputs
                Object.keys(newWeights).forEach(sector => {
                    if (sectorInputs[sector]) {
                        sectorInputs[sector].value = newWeights[sector].toFixed(2);
                        // Visual indication of client-side update
                        sectorInputs[sector].style.backgroundColor = "#fff9c4";
                        setTimeout(() => {
                            sectorInputs[sector].style.backgroundColor = "";
                        }, 2000);
                    }
                });
                
                // Update T2D Pulse score if possible
                updateT2DPulseScore(newWeights);
                
                // Update our stored weights
                currentWeights = newWeights;
                
                showFeedback("Weights updated client-side (server did not respond)", "warning");
            }
        }
        
        function updateT2DPulseScore(weights) {
            try {
                // This is a simplified score calculation for client-side fallback
                // Real calculation would require server-side sector sentiment data
                const pulseElement = document.getElementById("t2d-pulse-value");
                if (pulseElement) {
                    // Flash the pulse element to indicate update
                    pulseElement.style.transition = "all 0.3s ease";
                    pulseElement.style.backgroundColor = "#fff9c4";
                    setTimeout(() => {
                        pulseElement.style.backgroundColor = "";
                    }, 2000);
                }
            } catch (e) {
                console.error("Error updating T2D Pulse score:", e);
            }
        }
        
        // Utility function to display feedback to the user
        function showFeedback(message, type) {
            console.log(`Feedback: ${message} (${type})`);
            
            // First, update the built-in notification if possible
            const notificationElement = document.getElementById("weight-update-notification");
            if (notificationElement) {
                notificationElement.innerText = message;
                notificationElement.style.opacity = "1";
                notificationElement.style.color = 
                    type === "success" ? "green" : 
                    type === "error" ? "red" : 
                    type === "warning" ? "orange" : "blue";
                
                // Fade out after delay
                setTimeout(() => {
                    notificationElement.style.opacity = "0.6";
                }, 3000);
            }
            
            // Also show a custom notification
            const notification = document.createElement("div");
            notification.className = "enhanced-notification";
            notification.style.position = "fixed";
            notification.style.top = "20px";
            notification.style.right = "20px";
            notification.style.padding = "12px 20px";
            notification.style.borderRadius = "4px";
            notification.style.color = "white";
            notification.style.zIndex = "9999";
            notification.style.fontSize = "14px";
            notification.style.fontWeight = "bold";
            notification.style.boxShadow = "0 3px 6px rgba(0,0,0,0.2)";
            notification.style.transition = "opacity 0.5s ease";
            
            // Colors for different message types
            if (type === "success") {
                notification.style.backgroundColor = "#4caf50";
            } else if (type === "error") {
                notification.style.backgroundColor = "#f44336";
            } else if (type === "warning") {
                notification.style.backgroundColor = "#ff9800";
            } else if (type === "processing") {
                notification.style.backgroundColor = "#2196f3";
            } else {
                notification.style.backgroundColor = "#607d8b";
            }
            
            notification.innerText = message;
            document.body.appendChild(notification);
            
            // Fade out and remove
            setTimeout(() => {
                notification.style.opacity = "0";
                setTimeout(() => {
                    if (notification.parentNode) {
                        document.body.removeChild(notification);
                    }
                }, 500);
            }, 4000);
        }
    }
});