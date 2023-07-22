#include <stdio.h>
#include <stdlib.h>
#include <string.h>

// gen by gpt4-turbo
unsigned long convertHexString(const char *input) {
    const char *separator = "=";
    char *token;
    
    char *inputCopy = strdup(input);
    if (!inputCopy) return 0;

    token = strtok(inputCopy, separator);
    if (token != NULL) {
        token = strtok(NULL, separator);
        if (token != NULL) {
            char *end;
            for (end = token + strlen(token) - 1; end >= token && (*end == ' ' || *end == '\n'); --end) 
                *end = '\0';
            
            unsigned long number = (unsigned long)strtol(token, NULL, 16);
            free(inputCopy);
            return number;
        }
    }
    free(inputCopy);
    return 0;
}


unsigned long * get_media_writes() {
    unsigned long * ret = (unsigned long *) malloc(12*sizeof(unsigned long));
    FILE *fp;
    char content[2000];

    fp = popen("ipmctl show -performance | grep TotalMediaWrites", "r");
    if (fp == NULL) {
        printf("Failed to run ipmctl\n");
        exit(1);
    }
    int index = 0;
    while (fgets(content, sizeof(content), fp) != NULL) {
        printf("%s", content);
        unsigned long number = convertHexString(content);
        ret[index] = number;
        printf("%lu\n", number);
        index++;
    }
    pclose(fp);
    return ret;
}

int main()
{
    return 0;
}