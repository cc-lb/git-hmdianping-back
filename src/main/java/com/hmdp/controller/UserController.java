package com.hmdp.controller;


import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.RandomUtil;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.hmdp.dto.LoginFormDTO;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.User;
import com.hmdp.entity.UserInfo;
import com.hmdp.service.IUserInfoService;
import com.hmdp.service.IUserService;
import com.hmdp.utils.RegexUtils;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeanUtils;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.*;

import javax.annotation.Resource;
import javax.servlet.http.HttpSession;

import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.LOGIN_CODE_KEY;

/**
 * <p>
 * 前端控制器
 * </p>
 *
 * @author  cc
 */
@Slf4j
@RestController
@RequestMapping("/user")
public class UserController {

    @Resource
    private IUserService userService;

    @Resource
    private IUserInfoService userInfoService;
    @Resource
    private StringRedisTemplate stringRedisTemplate;

    /**
     * 发送手机验证码
     * 1，检验手机号码合不合格
     * 2，合格发送，不合格返回错误信息
     * 3.如果合格保存redis中等待校验
     */
    @PostMapping("code")
    public Result sendCode(@RequestParam("phone") String phone, HttpSession session) {

        if(RegexUtils.isPhoneInvalid(phone)){
            return Result.fail("手机号格式错误！");
        }
        String code = RandomUtil.randomNumbers(6);
        stringRedisTemplate.opsForValue().set(LOGIN_CODE_KEY+phone, code,3L, TimeUnit.MINUTES);

        //session.setAttribute("code", code);
       // stringRedisTemplate.opsForValue().set(,);
        log.debug("发送验证码成功:{}",code);

        return  Result.ok();
        // TODO 发送短信验证码并保存验证码
        //return Result.fail("功能未完成");
    }

    /**
     * 登录功能
     * @param loginForm 登录参数，包含手机号、验证码；或者手机号、密码
     *1，首先校验手机号是否正确
     *2,检验验证码，错误则返回错误信息
     *3，正确，判断数据库是否存在该用户（通过手机号），不存在则自动创建
     *
     *
     */
    @PostMapping("login")
    public Result login(@RequestBody LoginFormDTO loginForm, HttpSession session){
         return userService.
                 login(loginForm, session);




        /* Session版本，在Controller层中
         if(StringUtils.isEmpty(loginForm.getPhone())|| !RegexUtils.isPhoneInvalid(loginForm.getPhone())){
            return Result.fail("手机号码错误");
        }

        Object cacheCode = session.getAttribute("code");
        String code = loginForm.getCode();
        if(!cacheCode.equals(code)){
            return Result.fail("验证码错误");

        }
        User user = userService.getOne(new LambdaQueryWrapper<User>()
                .eq(User::getPhone, loginForm.getPhone().trim()));
        if(user==null){
            userService.save(user.setPhone(loginForm.getPhone()));
        }
           //user添加呢？？

        UserDTO userDTO = new UserDTO();
        BeanUtils.copyProperties(user, userDTO);
                session.setAttribute("user",userDTO);

         */
        // TODO 实现登录功能
        //return Result.fail("功能未完成");
    }

    /**
     * 登出功能
     * @return 无
     */
    @PostMapping("logout")
    public Result logout(){
        // TODO 实现登出功能
        return Result.fail("功能未完成");
    }

    @GetMapping("me")
    public Result me(){
        System.out.println("登录人：  "+UserHolder.getUser());
      if(UserHolder.getUser()!=null)
     return Result.ok(UserHolder.getUser());

        return Result.fail("未登录");
        // TODO 获取当前登录的用户并返回
        //return Result.fail("功能未完成");
    }

    @GetMapping("info/{id}")
    public Result info(@PathVariable("id") Long userId){
        // 查询详情
        UserInfo info = userInfoService.getById(userId);
        if (info == null) {
            // 没有详情，应该是第一次查看详情
            return Result.ok();
        }
        info.setCreateTime(null);
        info.setUpdateTime(null);
        // 返回
        return Result.ok(info);
    }

    @GetMapping("/{id}")
    public Result queryUserById(@PathVariable("id") Long userId){
        // 查询详情
        User user = userService.getById(userId);
        if (user == null) {
            return Result.ok();
        }
        UserDTO userDTO = BeanUtil.copyProperties(user, UserDTO.class);
        // 返回
        return Result.ok(userDTO);
    }

    @PostMapping("/sign")
    public Result sign(){
        return  userService.sign();
    }
    @GetMapping("/sign/count")
    public Result signCount(){
        return userService.signCount();
    }
}
