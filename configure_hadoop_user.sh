#!/bin/bash

passAnswersToPrompt()
{
        command=$1
    answers=$2

    printf $answers | $command
}

changeTextInFile()
{
    filePath=$1
    originalTxt=$2
    newTxt=$3

    sudo sed -i "'/"$originalTxt"/c\\"$newTxt"'" $filePath

}

publicKey=$1

passAnswersToPrompt "sudo passwd hadoop" 'hadoop123\nhadoop123'
sudo sed -i '/PasswordAuthentication no/c\PasswordAuthentication yes' /etc/ssh/sshd_config
sudo service ssh restart
sudo mkdir -p /home/hadoop/.ssh
echo $publicKey | sudo tee /home/hadoop/.ssh/authorized_keys 
sudo chown -R hadoop:hadoop /home/hadoop/.ssh
exit
