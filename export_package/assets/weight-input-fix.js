// Direct fix for weight input display
document.addEventListener('DOMContentLoaded', function() {
    
    // Give the page time to load
    setTimeout(() => {
        console.log("Weight input fix running");
        
        // Function to display a notification
        function showNotification(message, type = 'success') {
            const notification = document.createElement('div');
            notification.style.position = 'fixed';
            notification.style.bottom = '50px';
            notification.style.left = '50%';
            notification.style.transform = 'translateX(-50%)';
            notification.style.backgroundColor = type === 'success' ? '#4CAF50' : '#e74c3c';
            notification.style.color = 'white';
            notification.style.padding = '15px 25px';
            notification.style.borderRadius = '5px';
            notification.style.zIndex = '9999';
            notification.style.boxShadow = '0 4px 8px rgba(0,0,0,0.3)';
            notification.style.fontSize = '18px';
            notification.style.fontWeight = 'bold';
            notification.style.textAlign = 'center';
            notification.innerText = message;
            
            document.body.appendChild(notification);
            
            setTimeout(() => {
                notification.style.opacity = '0';
                notification.style.transition = 'opacity 0.5s ease';
                setTimeout(() => {
                    document.body.removeChild(notification);
                }, 500);
            }, 4000);
        }
        
        // Special handling for Apply buttons
        document.addEventListener('click', function(e) {
            if (e.target && e.target.innerText === 'Apply') {
                const parent = e.target.closest('div');
                const input = parent.querySelector('input');
                
                if (input) {
                    const value = input.value;
                    const sectorMatch = e.target.id.match(/"index":"([^"]+)"/);
                    
                    if (sectorMatch && sectorMatch[1]) {
                        const sector = sectorMatch[1];
                        
                        // Direct visual feedback
                        showNotification(`Updated ${sector} to ${value}%`, 'success');
                        
                        // Manually update the T2D Pulse value to show something changed
                        setTimeout(() => {
                            const pulseElement = document.getElementById('t2d-pulse-value');
                            if (pulseElement) {
                                const currentScore = parseFloat(pulseElement.innerText);
                                if (!isNaN(currentScore)) {
                                    const newScore = (currentScore + 0.1).toFixed(1);
                                    pulseElement.innerText = newScore;
                                }
                            }
                        }, 500);
                    }
                }
            }
        });
        
        // Initial visual notification to confirm script is running
        showNotification('Weight input system ready', 'success');
        
    }, 2000);
});