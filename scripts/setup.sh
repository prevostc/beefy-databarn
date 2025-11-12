#!/bin/bash
set -e

# Server setup script for Hetzner deployment
# Configures UFW firewall and fail2ban for rate limiting

echo "Setting up server security..."

# Update system
sudo apt-get update
sudo apt-get install -y ufw fail2ban

# Configure UFW firewall
echo "Configuring UFW firewall..."

# Default policies
sudo ufw default deny incoming
sudo ufw default allow outgoing

# Allow SSH (critical - don't lock yourself out!)
sudo ufw allow 22/tcp

# Allow HTTP and HTTPS
sudo ufw allow 80/tcp
sudo ufw allow 443/tcp

# Allow Docker Swarm ports
sudo ufw allow 2377/tcp  # Swarm management
sudo ufw allow 7946/tcp  # Container network discovery
sudo ufw allow 7946/udp  # Container network discovery
sudo ufw allow 4789/udp  # Overlay network traffic

# Enable UFW
echo "y" | sudo ufw enable

echo "✓ UFW firewall configured"

# Configure fail2ban
echo "Configuring fail2ban..."

sudo systemctl enable fail2ban
sudo systemctl start fail2ban

# Create fail2ban jail for SSH
sudo tee /etc/fail2ban/jail.local > /dev/null <<EOF
[DEFAULT]
bantime = 3600
findtime = 600
maxretry = 5

[sshd]
enabled = true
port = 22
filter = sshd
logpath = /var/log/auth.log
maxretry = 3
bantime = 7200
EOF

sudo systemctl restart fail2ban

echo "✓ fail2ban configured"

# Check status
echo ""
echo "UFW Status:"
sudo ufw status

echo ""
echo "fail2ban Status:"
sudo fail2ban-client status

echo ""
echo "✓ Server setup complete!"

