package main.java;

import java.util.ArrayList;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class Barbershop {

    private ArrayList<Client> queue = new ArrayList<>();
    private int clientsCounter = 0;
    private Lock queueManager = new ReentrantLock();
    private Condition noClients = queueManager.newCondition();
    private Barber barber = new Barber();

    private Barbershop() {
        this.barber.setName("Barber");
        this.barber.start();
    }

    private class Client extends Thread {

        int id;
        volatile boolean needsHaircut = true;

        private Client(int id) {
            this.id = id;
        }

        @Override
        public void run() {
            queueManager.lock();
            queue.add(this);
            System.out.println("Client " + this.id + " joined the barbershop!");
            if (queue.size() == 1) {
                noClients.signalAll();
            }
            queueManager.unlock();
            while (needsHaircut) {
                synchronized (this) {
                    try {
                        wait();
                    } catch (InterruptedException ignored) {
                    }
                }
            }
            System.out.println("Client " + this.id + " got a cool haircut!");
        }
    }

    private class Barber extends Thread {

        private void cutHair(Client client) throws InterruptedException {

            Thread.sleep((long) (1000 + Math.random() * 1000));
            synchronized (this) {
                synchronized (client) {
                    client.needsHaircut = false;
                    client.notify();
                }
                client.join();
            }
        }

        @Override
        public void run() {
            while (!Thread.interrupted()) {
                try {
                    queueManager.lock();
                    if (queue.size() == 0) {
                        System.out.println("[BARBER] No clients, barber is going to sleep... z..Zz.z..ZZz.z");
                        noClients.await();
                    }
                    Client client = queue.remove(0);
                    queueManager.unlock();
                    cutHair(client);
                } catch (InterruptedException ignored) {
                }
            }
        }
    }

    private void addClient() {
        Client newClient = new Client(this.clientsCounter);
        newClient.setName("Client " + this.clientsCounter++);
        newClient.start();
    }


    public static void main(String[] args) throws InterruptedException {

        Barbershop barbershop = new Barbershop();
        while (!Thread.interrupted()) {
            Thread.sleep((long) (1100 + Math.random() * 1000));
            barbershop.addClient();
        }
    }
}
