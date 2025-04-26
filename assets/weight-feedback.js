// Add client-side feedback for weight updates
window.addEventListener('DOMContentLoaded', (event) => {
    // Create a feedback message element
    const feedbackDiv = document.createElement('div');
    feedbackDiv.id = 'weight-update-feedback';
    feedbackDiv.style.position = 'fixed';
    feedbackDiv.style.top = '20px';
    feedbackDiv.style.right = '20px';
    feedbackDiv.style.padding = '12px 16px';
    feedbackDiv.style.backgroundColor = '#4CAF50';
    feedbackDiv.style.color = 'white';
    feedbackDiv.style.borderRadius = '4px';
    feedbackDiv.style.boxShadow = '0 4px 8px rgba(0,0,0,0.2)';
    feedbackDiv.style.zIndex = '1000';
    feedbackDiv.style.opacity = '0';
    feedbackDiv.style.transition = 'opacity 0.3s ease';
    feedbackDiv.style.fontSize = '16px';
    feedbackDiv.style.fontWeight = 'bold';
    document.body.appendChild(feedbackDiv);
    
    // Add click handler for all Apply buttons
    document.addEventListener('click', function(event) {
        // Check if the clicked element is an Apply button
        if (event.target && event.target.innerText === 'Apply') {
            // Find the associated input
            const parentDiv = event.target.closest('div');
            const inputDiv = parentDiv.querySelector('input');
            
            if (inputDiv) {
                const value = inputDiv.value;
                const sector = event.target.id.match(/"index":"([^"]+)"/)[1];
                
                // Show feedback
                feedbackDiv.innerText = `Updated ${sector} weight to ${value}%`;
                feedbackDiv.style.opacity = '1';
                
                // Clear feedback after 3 seconds
                setTimeout(() => {
                    feedbackDiv.style.opacity = '0';
                }, 3000);
            }
        }
    });
});