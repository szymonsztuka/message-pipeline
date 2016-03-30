package messagepipeline.example;

import messagepipeline.pipeline.node.UniversalNode;

import java.io.IOException;

import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.Files;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Path;
import java.nio.file.Paths;

public class CopyFile
{
    public static void main(String[] args)
    {
        for(String xa: args){
            System.out.println(xa) ;
        }

        if (args.length != 3)
        {
            System.out.println("usage: java Copy source target");
            return;
        }

        Path source = Paths.get(args[0]+"/"+args[2]);
        Path target = Paths.get(args[1]+"/"+args[2]);
        if(!Files.exists(target.getParent())){
            try {
                Files.createDirectories(target.getParent());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        try
        {
            Files.copy(source, target);
        }
        catch (FileAlreadyExistsException faee)
        {
            System.out.printf("%s: file already exists%n", target);
        }
        catch (DirectoryNotEmptyException dnee)
        {
            System.out.printf("%s: not empty%n", target);
        }
        catch (IOException ioe)
        {
            System.out.printf("I/O error: %s%n", ioe.getMessage());
        }
    }
}