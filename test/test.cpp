#include <iostream>
#include <thread>
#include <unistd.h>

using namespace std;

int main() {
    for(int i=0;i<=2;i++) {
        fork();
        printf("hello\n");
    }
}